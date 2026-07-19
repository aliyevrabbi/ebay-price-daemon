<<<<<<< HEAD
# eBay Price Deviation Daemon

Async daemon that scans eBay Buy-It-Now search results for configured
products, and fires a webhook when a listing's total price (item +
shipping) clears **both** a percentage-deviation threshold and an absolute
dollar-margin floor versus a known market value.

## Files

| File | Purpose |
|---|---|
| `main.py` | The whole daemon. No other local modules. |
| `config.json` | Your settings + product watchlist. Edit this, not `main.py`. |
| `requirements.txt` | `pip install -r requirements.txt` |
| `ebay-price-daemon.service` | systemd unit for running it as a VPS service |

## Quick start

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# edit config.json: set your real proxy_api_url token and webhook_url
nano config.json

# test one cycle before leaving it running unattended
python3 main.py --once --log-level DEBUG
```

If `--once` finds and alerts a deal, you'll see a `DEAL:` log line and a
POST to your webhook. Then run it for real:

```bash
python3 main.py
```

Ctrl+C triggers a clean shutdown: the current cycle's `seen_ids.json` is
flushed to disk before the process exits.

## Deploying as a systemd service

```bash
sudo useradd --system --home /opt/ebay-price-daemon --shell /usr/sbin/nologin ebaydaemon
sudo mkdir -p /opt/ebay-price-daemon /var/log/ebay-price-daemon
sudo cp main.py config.json requirements.txt /opt/ebay-price-daemon/
sudo chown -R ebaydaemon:ebaydaemon /opt/ebay-price-daemon /var/log/ebay-price-daemon

cd /opt/ebay-price-daemon
sudo -u ebaydaemon python3 -m venv venv
sudo -u ebaydaemon venv/bin/pip install -r requirements.txt

sudo cp ebay-price-daemon.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now ebay-price-daemon
sudo journalctl -u ebay-price-daemon -f
```

## Config reference (`config.json`)

```jsonc
{
  "settings": {
    "global_deviation_threshold_percent": 0.15,  // 15% minimum discount vs market value
    "min_absolute_profit_usd": 30.00,             // AND at least $30 margin in absolute terms
    "check_interval_seconds": 180,                // time between scan cycles
    "proxy_api_url": "http://api.scrape.do/?token=YOUR_TOKEN&url=",
    "webhook_url": "YOUR_WEBHOOK_URL",
    "request_timeout_seconds": 15,
    "max_retries": 3,
    "retry_backoff_seconds": 2.0,
    "results_per_page": 60
  },
  "products": [
    {
      "name": "Steam Deck",
      "base_market_value": 350.00,
      "required_keywords": ["steam", "deck"],
      "negative_keywords": ["box", "case", "broken", "faulty", "parts", "screen", "repair", "accessory", "skin"]
    }
  ]
}
```

Both `required_keywords` and `negative_keywords` are matched against the
listing title with `\bword\b` (case-insensitive) — `steam` will not match
inside `steampunk`, `deck` will not match inside `decking`.

Add more products by appending more objects to `products`; they're all
scanned concurrently every cycle regardless of how many you add.

## How the gatekeeper works

For every candidate listing:

```
total_price       = item_price + shipping_price
margin_usd        = base_market_value - total_price
deviation_percent = margin_usd / base_market_value

alert only if: deviation_percent >= global_deviation_threshold_percent
           AND margin_usd        >= min_absolute_profit_usd
```

The `min_absolute_profit_usd` floor is what stops the "percentage trap":
a $20 item at $5 is a 75% deviation but only a $15 margin — not worth
alerting on. A $350 item at $290 is "only" ~17% but a real $60 margin.
Both checks have to pass.

## Parsing notes / things that will eventually need attention

eBay's search-results markup is not stable long-term. As of mid-2026 eBay
serves either the newer `li.s-card[data-listingid]` layout or the older
`li.s-item` layout depending on the request. `main.py` detects which one
it got and parses accordingly (`_parse_s_card` / `_parse_s_item`); a
listing whose price is a range (e.g. `$25.00 to $45.00`, meaning it has
unselected variations) is intentionally discarded rather than guessed at.

If eBay changes its markup again and the daemon starts logging `0
listing(s) parsed` for queries that clearly have results, that's your
signal to open `--log-level DEBUG`, save a sample of the raw HTML your
proxy is returning, and update the CSS selectors in those two functions.
Everything downstream of them (keyword filtering, price math, alerting,
memory) is layout-agnostic and won't need to change.

## Scope note

This tool scrapes eBay's public search-results pages through whatever
proxy/unlocker service you configure — it doesn't try to defeat eBay's
bot detection itself. Keep `check_interval_seconds` reasonable and make
sure your use complies with eBay's Terms of Use and your proxy provider's
terms.
=======
# ebay-price-daemon
Self-hosted eBay price monitor daemon with Telegram alerts and atomic config management.
>>>>>>> bc7e0c48dfec3822f34b14670b7297b1a2d5b4be
