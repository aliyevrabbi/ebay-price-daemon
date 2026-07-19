#!/usr/bin/env python3
"""
eBay Price Deviation Daemon
============================

Concurrently scans eBay Buy-It-Now search results for a configured list of
products, flags listings that are priced meaningfully below a known market
value, and fires a webhook for each one. Built to run unattended on a Linux
VPS under systemd.

Design notes (read this before deploying):

* eBay's search-results markup changes on a rolling basis. As of mid-2026
  eBay is mid-migration from the old `li.s-item` card layout to a new
  `li.s-card` layout, and serves either one depending on the request/AB
  bucket. This daemon detects and parses BOTH. If eBay reshuffles class
  names again, `_parse_s_card` / `_parse_s_item` are the only two functions
  you should need to touch -- everything downstream works on the normalized
  `RawListing` dataclass.
* This scrapes eBay through whatever HTTP proxy/unlocker service you put in
  `proxy_api_url` (e.g. scrape.do). It does not attempt to bypass eBay's
  bot detection itself -- that's the proxy's job. You are responsible for
  using this in a way that complies with eBay's Terms of Use and your proxy
  provider's terms, and for keeping request rates reasonable.
* "Deals" are only ever alerted once. An item that alerts is added to
  seen_ids.json so it isn't re-sent every cycle. An item that DOESN'T
  currently clear the profit bar is deliberately left out of seen_ids, so
  if the seller drops the price later, it will still get evaluated (and
  potentially alert) on a future cycle.
"""

from __future__ import annotations

import argparse
import asyncio
import functools
import json
import logging
import re
import signal
import sys
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Deque, List, Optional, Set, Tuple
from urllib.parse import quote, urlencode

import aiohttp
from bs4 import BeautifulSoup

# --------------------------------------------------------------------------
# Constants
# --------------------------------------------------------------------------

SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_CONFIG_PATH = SCRIPT_DIR / "config.json"
DEFAULT_SEEN_IDS_PATH = SCRIPT_DIR / "seen_ids.json"
DEFAULT_LOG_PATH = SCRIPT_DIR / "daemon.log"

SEEN_IDS_MAXLEN = 1000

EBAY_SEARCH_BASE = "https://www.ebay.com/sch/i.html"

# Fixed, mandatory eBay query-string parameters per spec:
#   LH_BIN=1                        -> Buy It Now only (no auctions)
#   _sop=10                         -> sort by Price + shipping: lowest first
#   LH_ItemCondition=1000,...,3000  -> New, New other, Certified refurb,
#                                       Excellent refurb, Very good refurb
#   _curr=USD                       -> force USD pricing
#   rt=nc                           -> disable eBay's result caching
#   _dmd=1                          -> list view (stable markup, no mixed
#                                       grid/gallery layouts)
EBAY_FIXED_PARAMS = {
    "LH_BIN": "1",
    "_sop": "10",
    "LH_ItemCondition": "1000,1500,2000,2500,3000",
    "_curr": "USD",
    "rt": "nc",
    "_dmd": "1",
}

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

# Placeholder / non-listing cards eBay injects into search results.
IGNORED_TITLES = {
    "shop on ebay",
    "results matching fewer words",
}

PRICE_PATTERN = re.compile(r"\$\s?([\d,]+(?:\.\d{1,2})?)")
RANGE_PATTERN = re.compile(r"\bto\b", re.IGNORECASE)
ITEM_ID_FROM_URL = re.compile(r"/itm/(?:[^/?]+/)?(\d+)")
RETRYABLE_HTTP_STATUS = {408, 425, 429, 500, 502, 503, 504}

logger = logging.getLogger("ebay_daemon")


# --------------------------------------------------------------------------
# Config models
# --------------------------------------------------------------------------

@dataclass
class GlobalSettings:
    global_deviation_threshold_percent: float
    min_absolute_profit_usd: float
    check_interval_seconds: int
    proxy_api_url: str
    webhook_url: str
    request_timeout_seconds: int = 15
    max_retries: int = 3
    retry_backoff_seconds: float = 2.0
    results_per_page: int = 60


@dataclass
class ProductConfig:
    name: str
    base_market_value: float
    required_keywords: List[str]
    negative_keywords: List[str]
    required_patterns: List[re.Pattern] = field(init=False, repr=False)
    negative_patterns: List[re.Pattern] = field(init=False, repr=False)

    def __post_init__(self) -> None:
        self.required_patterns = _compile_word_boundary_patterns(self.required_keywords)
        self.negative_patterns = _compile_word_boundary_patterns(self.negative_keywords)


def _compile_word_boundary_patterns(keywords: List[str]) -> List[re.Pattern]:
    """Word-boundary, case-insensitive patterns so 'steam' never matches
    inside 'steampunk', 'deck' never matches inside 'decking', etc."""
    return [re.compile(rf"\b{re.escape(kw)}\b", re.IGNORECASE) for kw in keywords]


def load_config(path: Path) -> Tuple[GlobalSettings, List[ProductConfig]]:
    if not path.exists():
        raise SystemExit(
            f"Config file not found: {path}\n"
            f"Copy config.json into place next to main.py and edit it first."
        )

    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise SystemExit(f"Config file {path} is not valid JSON: {exc}") from exc

    settings_raw = raw.get("settings", {})
    required_settings_keys = (
        "global_deviation_threshold_percent",
        "min_absolute_profit_usd",
        "check_interval_seconds",
        "proxy_api_url",
        "webhook_url",
    )
    missing = [k for k in required_settings_keys if k not in settings_raw]
    if missing:
        raise SystemExit(f"config.json 'settings' is missing required key(s): {missing}")

    settings = GlobalSettings(
        global_deviation_threshold_percent=float(settings_raw["global_deviation_threshold_percent"]),
        min_absolute_profit_usd=float(settings_raw["min_absolute_profit_usd"]),
        check_interval_seconds=int(settings_raw["check_interval_seconds"]),
        proxy_api_url=str(settings_raw["proxy_api_url"]),
        webhook_url=str(settings_raw["webhook_url"]),
        request_timeout_seconds=int(settings_raw.get("request_timeout_seconds", 15)),
        max_retries=int(settings_raw.get("max_retries", 3)),
        retry_backoff_seconds=float(settings_raw.get("retry_backoff_seconds", 2.0)),
        results_per_page=int(settings_raw.get("results_per_page", 60)),
    )

    products_raw = raw.get("products", [])
    if not products_raw:
        raise SystemExit("config.json has no products configured under 'products'.")

    products: List[ProductConfig] = []
    for i, p in enumerate(products_raw):
        for key in ("name", "base_market_value", "required_keywords"):
            if key not in p:
                raise SystemExit(f"config.json products[{i}] is missing required key '{key}'")
        products.append(
            ProductConfig(
                name=str(p["name"]),
                base_market_value=float(p["base_market_value"]),
                required_keywords=[str(k) for k in p.get("required_keywords", [])],
                negative_keywords=[str(k) for k in p.get("negative_keywords", [])],
            )
        )

    for placeholder, attr in (("YOUR_TOKEN", "proxy_api_url"), ("YOUR_WEBHOOK_URL", "webhook_url")):
        if placeholder in getattr(settings, attr):
            logger.warning(
                "settings.%s still contains the placeholder %r -- update config.json before deploying.",
                attr, placeholder,
            )

    return settings, products


# --------------------------------------------------------------------------
# Persistent "seen" memory (dedupe alerts across restarts / cycles)
# --------------------------------------------------------------------------

class SeenIDStore:
    """A collections.deque(maxlen=1000) of alerted item IDs, persisted to
    disk as JSON. A parallel set gives O(1) membership checks; the deque
    keeps eviction order so the store never exceeds maxlen."""

    def __init__(self, path: Path, maxlen: int = SEEN_IDS_MAXLEN) -> None:
        self.path = path
        self._ids: Deque[str] = deque(maxlen=maxlen)
        self._id_set: Set[str] = set()
        self._load()

    def _load(self) -> None:
        if not self.path.exists():
            return
        try:
            data = json.loads(self.path.read_text(encoding="utf-8"))
            if isinstance(data, list):
                for item_id in data:
                    self._remember(str(item_id))
                logger.info("Loaded %d seen id(s) from %s", len(self._ids), self.path)
        except (json.JSONDecodeError, OSError) as exc:
            logger.warning("Could not load %s (%s) -- starting with empty memory.", self.path, exc)

    def _remember(self, item_id: str) -> None:
        if item_id in self._id_set:
            return
        if self._ids.maxlen is not None and len(self._ids) >= self._ids.maxlen:
            evicted = self._ids[0]  # this is what append() is about to push out
            self._id_set.discard(evicted)
        self._ids.append(item_id)
        self._id_set.add(item_id)

    def has_seen(self, item_id: str) -> bool:
        return item_id in self._id_set

    def mark_seen(self, item_id: str) -> None:
        self._remember(item_id)

    def save(self) -> None:
        tmp_path = self.path.with_suffix(self.path.suffix + ".tmp")
        try:
            tmp_path.write_text(json.dumps(list(self._ids)), encoding="utf-8")
            tmp_path.replace(self.path)  # atomic on POSIX
        except OSError as exc:
            logger.error("Failed to persist seen_ids to %s: %s", self.path, exc)

    def __len__(self) -> int:
        return len(self._ids)


# --------------------------------------------------------------------------
# URL construction
# --------------------------------------------------------------------------

def build_ebay_search_url(product: ProductConfig, settings: GlobalSettings) -> str:
    params = dict(EBAY_FIXED_PARAMS)
    params["_nkw"] = " ".join(product.required_keywords)
    params["_ipg"] = str(settings.results_per_page)
    # keep the LH_ItemCondition commas human-readable; not functionally required
    return f"{EBAY_SEARCH_BASE}?{urlencode(params, safe=',')}"


def build_proxy_url(target_url: str, settings: GlobalSettings) -> str:
    return f"{settings.proxy_api_url}{quote(target_url, safe='')}"


# --------------------------------------------------------------------------
# Async fetch with retry/backoff
# --------------------------------------------------------------------------

async def fetch_html(
    session: aiohttp.ClientSession,
    url: str,
    max_retries: int,
    backoff_base_seconds: float,
    timeout_seconds: int,
) -> str:
    """GET url and return response text. Retries on timeouts, connection
    errors, and retryable HTTP statuses (429/5xx) with exponential backoff.
    Fails fast (no retry) on non-retryable client errors like a bad proxy
    token (401/403) so we don't burn the retry budget on a config problem.
    """
    timeout = aiohttp.ClientTimeout(total=timeout_seconds)
    last_error: Optional[BaseException] = None

    for attempt in range(1, max_retries + 1):
        try:
            async with session.get(url, timeout=timeout) as response:
                if response.status == 200:
                    return await response.text()

                preview = (await response.text())[:200]
                last_error = RuntimeError(f"HTTP {response.status}: {preview!r}")
                if response.status not in RETRYABLE_HTTP_STATUS:
                    break
        except asyncio.TimeoutError as exc:
            last_error = exc
        except aiohttp.ClientError as exc:
            last_error = exc

        if attempt < max_retries:
            delay = backoff_base_seconds * (2 ** (attempt - 1))
            logger.warning(
                "Fetch attempt %d/%d failed (%s). Retrying in %.1fs...",
                attempt, max_retries, last_error, delay,
            )
            await asyncio.sleep(delay)

    raise ConnectionError(f"Exhausted {max_retries} attempt(s) fetching via proxy: {last_error}")


async def send_webhook(session, webhook_url, payload, max_retries, timeout_seconds):
    timeout = aiohttp.ClientTimeout(total=timeout_seconds)
    
    # 1. Mesajı Telegram-ın istədiyi 'text' formatına çeviririk (HTML formatında)
    message_text = (
        f"🔥 <b>DEAL FOUND</b>\n"
        f"<b>Title:</b> {payload.get('title', 'Unknown')}\n"
        f"<b>Price:</b> ${payload.get('total_price_usd', 0)}\n"
        f"<b>Margin:</b> ${payload.get('margin_usd', 0)} ({payload.get('deviation_percent', 0)}%)\n"
        f"<a href='{payload.get('url', '#')}'>View on eBay</a>"
    )

    # 2. Telegram-ın gözlədiyi JSON strukturu
    telegram_payload = {
        "chat_id": "5012068984", 
        "text": message_text,
        "parse_mode": "HTML"
    }

    # 3. URL-i təmizlə (əgər URL-də ?chat_id= varsa, onu silirik, çünki JSON-da göndəririk)
    base_url = webhook_url.split('?')[0]

    last_error = None
    for attempt in range(1, max_retries + 1):
        try:
            # İndi 'json=telegram_payload' göndəririk, bütün məlumatlar bunun içindədir
            async with session.post(base_url, json=telegram_payload, timeout=timeout) as response:
                if response.status < 300:
                    return True
                else:
                    # Xəta olduqda Telegram nə deyir? Bunu loglayaq
                    error_text = await response.text()
                    logger.error(f"Telegram returned error {response.status}: {error_text}")
                    last_error = RuntimeError(f"HTTP {response.status}")
        except Exception as e:
            last_error = e
        
        if attempt < max_retries:
            delay = 2.0 * attempt
            logger.warning("Webhook attempt %d/%d failed (%s). Retrying in %.1fs...", attempt, max_retries, last_error, delay)
            await asyncio.sleep(delay)
            
    return False


# --------------------------------------------------------------------------
# Parsing: eBay search-results HTML -> normalized listings
# --------------------------------------------------------------------------

@dataclass
class RawListing:
    item_id: str
    title: str
    url: str
    price_text: Optional[str]
    shipping_text: Optional[str]


def _parse_s_card(card) -> Optional[RawListing]:
    """New (2026) layout: <li class="s-card" data-listingid="...">"""
    item_id = card.get("data-listingid")
    title_el = card.select_one(".s-card__title")
    title = title_el.get_text(" ", strip=True) if title_el else ""
    if not item_id or not title or title.lower() in IGNORED_TITLES:
        return None

    link_el = card.select_one("a.su-link") or card.select_one("a[href*='/itm/']")
    url = link_el.get("href", "").split("?")[0] if link_el else ""

    price_text: Optional[str] = None
    shipping_text: Optional[str] = None
    for row in card.select(".s-card__attribute-row"):
        row_text = row.get_text(" ", strip=True)
        if not row_text:
            continue
        lowered = row_text.lower()
        if "shipping" in lowered or "delivery" in lowered:
            if shipping_text is None:
                shipping_text = row_text
        elif "$" in row_text and price_text is None:
            price_text = row_text

    if price_text is None:
        price_el = card.select_one(".s-card__price")
        price_text = price_el.get_text(" ", strip=True) if price_el else None

    return RawListing(item_id=item_id, title=title, url=url, price_text=price_text, shipping_text=shipping_text)


def _parse_s_item(item) -> Optional[RawListing]:
    """Legacy layout: <li class="s-item">"""
    title_el = item.select_one(".s-item__title")
    title = title_el.get_text(" ", strip=True) if title_el else ""
    if not title or title.lower() in IGNORED_TITLES:
        return None

    link_el = item.select_one("a.s-item__link")
    url = link_el.get("href", "").split("?")[0] if link_el else ""
    id_match = ITEM_ID_FROM_URL.search(url)
    if not id_match:
        return None
    item_id = id_match.group(1)

    price_el = item.select_one(".s-item__price")
    price_text = price_el.get_text(" ", strip=True) if price_el else None

    shipping_el = item.select_one(".s-item__shipping, .s-item__logisticsCost")
    shipping_text = shipping_el.get_text(" ", strip=True) if shipping_el else None

    return RawListing(item_id=item_id, title=title, url=url, price_text=price_text, shipping_text=shipping_text)


def extract_listings(html: str) -> List[RawListing]:
    soup = BeautifulSoup(html, "html.parser")
    results: List[RawListing] = []

    new_layout_cards = soup.select("li.s-card[data-listingid]")
    if new_layout_cards:
        for card in new_layout_cards:
            parsed = _parse_s_card(card)
            if parsed:
                results.append(parsed)
        return results

    legacy_items = soup.select("li.s-item")
    for item in legacy_items:
        parsed = _parse_s_item(item)
        if parsed:
            results.append(parsed)
    return results


def parse_price(text: Optional[str]) -> Optional[float]:
    """Parse a listing's price text into a float. Returns None for missing,
    unparseable, or ranged/variation prices ("$25.00 to $45.00") -- those
    are multi-variant listings without one fixed price and must be
    discarded per spec rather than guessed at."""
    if not text:
        return None
    if RANGE_PATTERN.search(text):
        return None
    match = PRICE_PATTERN.search(text)
    if not match:
        return None
    try:
        return float(match.group(1).replace(",", ""))
    except ValueError:
        return None


def parse_shipping(text: Optional[str]) -> float:
    """Parse shipping cost text. 'Free' shipping, or a missing shipping
    element entirely (which on eBay search cards almost always means free),
    both resolve to 0.0."""
    if not text:
        return 0.0
    if re.search(r"\bfree\b", text, re.IGNORECASE):
        return 0.0
    match = PRICE_PATTERN.search(text)
    if match:
        try:
            return float(match.group(1).replace(",", ""))
        except ValueError:
            return 0.0
    return 0.0


def passes_keyword_filter(title: str, required_patterns: List[re.Pattern], negative_patterns: List[re.Pattern]) -> bool:
    if not all(p.search(title) for p in required_patterns):
        return False
    if any(p.search(title) for p in negative_patterns):
        return False
    return True


# --------------------------------------------------------------------------
# Profit gatekeeper + alerting
# --------------------------------------------------------------------------

async def evaluate_and_alert(
    session: aiohttp.ClientSession,
    product: ProductConfig,
    settings: GlobalSettings,
    listing: RawListing,
    seen_store: SeenIDStore,
) -> None:
    item_price = parse_price(listing.price_text)
    if item_price is None:
        logger.debug(
            "[%s] Skip %s: no single fixed price (variation/dropdown or unparseable: %r)",
            product.name, listing.item_id, listing.price_text,
        )
        return

    shipping_price = parse_shipping(listing.shipping_text)
    total_price = item_price + shipping_price

    margin_usd = product.base_market_value - total_price
    deviation_percent = (margin_usd / product.base_market_value) if product.base_market_value else 0.0

    # --- The gatekeeper: BOTH conditions must hold. This is what stops the
    # "percentage profit trap" on cheap items (e.g. 90% off a $5 item is a
    # meaningless $4.50 margin) while still catching genuinely marginal
    # deals on expensive items that fail a pure-percentage check. ---
    if deviation_percent < settings.global_deviation_threshold_percent:
        return
    if margin_usd < settings.min_absolute_profit_usd:
        return

    logger.info(
        "[%s] DEAL: %s | total=$%.2f market=$%.2f margin=$%.2f (%.1f%%)",
        product.name, listing.title[:80], total_price, product.base_market_value,
        margin_usd, deviation_percent * 100,
    )

    payload = {
        "product": product.name,
        "item_id": listing.item_id,
        "title": listing.title,
        "url": listing.url,
        "item_price_usd": round(item_price, 2),
        "shipping_price_usd": round(shipping_price, 2),
        "total_price_usd": round(total_price, 2),
        "base_market_value_usd": round(product.base_market_value, 2),
        "margin_usd": round(margin_usd, 2),
        "deviation_percent": round(deviation_percent * 100, 2),
        "detected_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }

    delivered = await send_webhook(
        session, settings.webhook_url, payload, settings.max_retries, settings.request_timeout_seconds
    )
    if delivered:
        # Only mark as seen once the alert actually goes out. If the
        # webhook is down, we deliberately retry this same item next cycle
        # instead of silently losing the alert.
        seen_store.mark_seen(listing.item_id)
        logger.info("[%s] Webhook delivered for item %s", product.name, listing.item_id)
    else:
        logger.error("[%s] Webhook delivery FAILED for item %s -- will retry next cycle", product.name, listing.item_id)


async def scan_product(
    session: aiohttp.ClientSession,
    product: ProductConfig,
    settings: GlobalSettings,
    seen_store: SeenIDStore,
) -> None:
    target_url = build_ebay_search_url(product, settings)
    proxy_url = build_proxy_url(target_url, settings)
    logger.debug("[%s] GET (via proxy) -> %s", product.name, target_url)

    try:
        html = await fetch_html(
            session, proxy_url, settings.max_retries, settings.retry_backoff_seconds, settings.request_timeout_seconds
        )
    except ConnectionError as exc:
        logger.error("[%s] Giving up on this cycle: %s", product.name, exc)
        return

    listings = extract_listings(html)
    logger.info("[%s] Parsed %d listing(s) from search results", product.name, len(listings))

    candidates = [
        listing
        for listing in listings
        if not seen_store.has_seen(listing.item_id)
        and passes_keyword_filter(listing.title, product.required_patterns, product.negative_patterns)
    ]

    if not candidates:
        return

    logger.info("[%s] %d candidate(s) passed keyword filtering, checking profit margin...", product.name, len(candidates))
    await asyncio.gather(*(
        evaluate_and_alert(session, product, settings, listing, seen_store) for listing in candidates
    ))


async def run_scan_cycle(
    session: aiohttp.ClientSession,
    settings: GlobalSettings,
    products: List[ProductConfig],
    seen_store: SeenIDStore,
) -> None:
    """Scans every configured product concurrently -- this is the whole
    point of the async rewrite: N products means N in-flight requests, not
    N sequential blocking calls."""
    results = await asyncio.gather(
        *(scan_product(session, product, settings, seen_store) for product in products),
        return_exceptions=True,
    )
    for product, result in zip(products, results):
        if isinstance(result, Exception):
            logger.error("[%s] Unhandled exception during scan: %s", product.name, result, exc_info=result)


# --------------------------------------------------------------------------
# Daemon loop + graceful shutdown
# --------------------------------------------------------------------------

async def run_daemon(
    settings: GlobalSettings,
    products: List[ProductConfig],
    seen_store: SeenIDStore,
    run_once: bool = False,
) -> None:
    connector = aiohttp.TCPConnector(limit=30, limit_per_host=10)
    timeout = aiohttp.ClientTimeout(total=settings.request_timeout_seconds)

    async with aiohttp.ClientSession(connector=connector, timeout=timeout, headers=DEFAULT_HEADERS) as session:
        while True:
            cycle_start = time.monotonic()
            logger.info("=== Scan cycle starting (%d product[s]) ===", len(products))
            await run_scan_cycle(session, settings, products, seen_store)
            seen_store.save()
            elapsed = time.monotonic() - cycle_start

            if run_once:
                logger.info("Cycle finished in %.1fs. --once was set, exiting.", elapsed)
                return

            sleep_for = max(1.0, settings.check_interval_seconds - elapsed)
            logger.info("Cycle finished in %.1fs. Sleeping %.1fs until next cycle.", elapsed, sleep_for)
            await asyncio.sleep(sleep_for)


async def main_async(args: argparse.Namespace) -> None:
    settings, products = load_config(args.config)
    seen_store = SeenIDStore(args.seen_ids, maxlen=SEEN_IDS_MAXLEN)

    logger.info("Loaded %d product(s): %s", len(products), ", ".join(p.name for p in products))
    logger.info(
        "Gatekeeper: deviation >= %.1f%% AND margin >= $%.2f",
        settings.global_deviation_threshold_percent * 100, settings.min_absolute_profit_usd,
    )

    main_task = asyncio.current_task()
    loop = asyncio.get_running_loop()
    if not args.once:
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, functools.partial(_handle_shutdown_signal, sig, main_task))
            except NotImplementedError:
                # add_signal_handler isn't available on some platforms (e.g. Windows);
                # KeyboardInterrupt still works via the try/except in main() below.
                pass

    try:
        await run_daemon(settings, products, seen_store, run_once=args.once)
    except asyncio.CancelledError:
        logger.info("Shutdown signal received. Flushing memory to disk before exit...")
        raise
    finally:
        seen_store.save()
        logger.info("Saved %d seen id(s) to %s. Goodbye.", len(seen_store), seen_store.path)


def _handle_shutdown_signal(sig: signal.Signals, task: "asyncio.Task[None]") -> None:
    logger.info("Received %s", sig.name)
    task.cancel()


def setup_logging(level: str, log_path: Path) -> None:
    handlers: List[logging.Handler] = [logging.StreamHandler(sys.stdout)]
    try:
        handlers.append(logging.FileHandler(log_path, encoding="utf-8"))
    except OSError as exc:
        print(f"Warning: could not open log file {log_path} ({exc}); logging to stdout only.", file=sys.stderr)

    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s | %(levelname)-8s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=handlers,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="eBay Price Deviation Daemon")
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG_PATH, help="Path to config.json")
    parser.add_argument("--seen-ids", type=Path, default=DEFAULT_SEEN_IDS_PATH, help="Path to seen_ids.json")
    parser.add_argument("--log-file", type=Path, default=DEFAULT_LOG_PATH, help="Path to daemon.log")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    parser.add_argument("--once", action="store_true", help="Run a single scan cycle and exit (for testing).")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    setup_logging(args.log_level, args.log_file)
    try:
        asyncio.run(main_async(args))
    except KeyboardInterrupt:
        # Fallback path (e.g. platforms without add_signal_handler support).
        logger.info("Interrupted. Exiting.")


if __name__ == "__main__":
    main()
