# Live Momentum Scanner

This folder now contains a browser UI plus a small Kite-backed server. The server keeps Kite credentials on the backend and exposes only calculated scanner rows to the page.

## Start locally

From this folder:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements-live.txt
export KITE_API_KEY="your_kite_api_key"
export KITE_ACCESS_TOKEN="your_daily_access_token"
export GOOGLE_CLIENT_ID="your_google_web_client_id"
export RAZORPAY_KEY_ID="your_razorpay_key_id"
export RAZORPAY_KEY_SECRET="your_razorpay_key_secret"
export ACCESS_COOKIE_SECURE="false"
python3 live_scanner_server.py
```

Open `http://127.0.0.1:8050/` in a browser. Do not open the HTML file directly if you want live values; the page must be served by `live_scanner_server.py` so it can call `/api/scan`.

## What the server does

- Loads the supplied NSE sector universe from `sector_definitions.py`.
- By default, watches and calculates detailed indicators for every stock in the configured NSE universe.
- Sector flow is calculated separately from quote ticks across the whole NSE universe.
- Uses Kite historical candles to seed 5-minute and daily indicators.
- Uses KiteTicker full-mode ticks for current price, volume, and five-point sparklines.
- Calculates RSI, ADX, 21 EMA distance, time-adjusted volume ratio, recent-bar continuation, session trend quality, volume confirmation, RFactor, volatility bucket, and momentum rank. RFactor matches the supplied `dashboard_clean.py` formula: 20-session volume/range/move baselines, 0.55/0.30/0.15 weighting, price-position freshness, narrow-range penalty, and logarithmic scaling. RFactor and directional continuation directly affect rank, so an early spike loses rank when the stock goes sideways instead of continuing.
- Calculates and exposes a live RFactor value; click any Sector flow bar to see its stocks sorted by RFactor. Sector flow has button controls for average percentage change or average intraday volume ratio. Both modes use automatic spacing that gives more vertical space to the side with greater total flow; VolmRatio mode uses raw VolmRatio mean for height, %CHG mode uses %CHG mean for height, and colors follow sector %CHG mean. Sector bars stay on one line without horizontal scrolling.
- Provides a Volm Ratio view with positive-%CHG stocks in Top Gainers and negative-%CHG stocks in Top Decliners; both lists are sorted by VolmRatio high-to-low and show the first 10 rows before scrolling.
- Ranks positive and negative movers together by composite momentum score, keeping one continuous rank sequence so green and red names appear together.
- Shows the cumulative KiteTicker tick count beside the feed status.
- Serves `/api/scan` for the page and `/api/health` for feed status.
- Verifies Google Identity Services ID tokens server-side and stores signed-in users in SQLite.
- Creates Razorpay orders server-side and verifies the Razorpay HMAC signature before granting access.
- Grants six-month access for ₹4,999 by default (`SUBSCRIPTION_PRICE_PAISE` and `SUBSCRIPTION_MONTHS` can override it).
- Stores users, sessions, payment orders, and paid-until entitlements in `AUTH_DB_PATH` (default: `SCANNER_DATA_DIR/access.sqlite3`).
- Requires an authenticated, currently paid session for `/api/scan`, so live scanner data is not returned to an unpaid page.
- Starts market-data initialization in the background under Uvicorn, so the dashboard opens while history is still seeding.
- Recomputes detailed rows and whole-universe Sector flow in a background cache; `/api/scan` only reads that cache, so browser polling does not rerun indicators or RFactor calculations.
- Detects the next calendar session, clears prior-session live ticks, and reseeds fresh Kite history automatically without requiring a Render restart; the previous-session cache stays visible while this happens.
- Outside market hours, the page labels loaded data as `Previous session` until the next session begins.
- If the service restarts off-hours, the intraday view loads the persisted latest completed-session OHLC and volume cache, so ranking still reflects that session’s momentum without a redundant full seed.
- A weekday is not treated as a new trading session until current-session candles or ticks actually exist, so weekends and exchange holidays continue showing the latest completed session.
- When current-session data is absent, continuation, volume confirmation, and volume ratio all use the latest available trading session rather than the calendar date.
- The pre-market seed starts at `07:30 IST` by default (`PREMARKET_SEED_TIME` can change it), so current history is normally ready before the `09:15 IST` open.

Full-universe mode is enabled by default. Set `FAST_MODE=true` to optionally watch all stocks with lightweight quote ticks while limiting detailed history/order-book work to `FAST_SYMBOL_LIMIT` stocks. Fast mode intentionally refreshes newly selected symbols as the rotation changes; keep `FAST_MODE=false` to avoid that behavior. `FAST_SELECTION_WAIT_SEC` controls how long startup waits for live quotes before selecting the Fast mode list, and `FAST_RESELECT_SEC` controls how often that list rotates (default: 300 seconds).
`SCAN_COMPUTE_EVERY_SEC` controls the background cache refresh interval (default: 8 seconds).

The service keeps a best-effort local history cache when `SCANNER_DATA_DIR` is available, but the default Render filesystem is ephemeral. The same running instance will not repeat a completed seed on the same day; a true Render restart requires a fresh seed unless an external or persistent storage service is configured.

The initial historical seed is deliberately paced and can take a few minutes for the full universe. The lightweight defaults use 7 days of 5-minute candles and 120 days of daily candles. Until enough history is available, the page remains in demo mode. Kite access tokens normally expire daily, so provide a fresh token before starting the server.

For a single-process production deployment, use one worker because the process owns one KiteTicker connection:

```bash
gunicorn app:app --bind 0.0.0.0:8050 --workers 1 --worker-class gthread --threads 8 --timeout 120
```

## Deploy on Render

1. Put the files in this folder in a GitHub repository.
2. In Render, create a new Web Service from that repository.
3. Set the Root Directory to the folder containing `app.py` and `requirements.txt`.
4. Use Build Command: `python3 -m pip install -r requirements.txt`.
5. Use Start Command: `gunicorn app:app --bind 0.0.0.0:$PORT --workers 1 --worker-class gthread --threads 8 --timeout 120`.
6. Add `KITE_API_KEY`, `KITE_ACCESS_TOKEN`, `GOOGLE_CLIENT_ID`, `RAZORPAY_KEY_ID`, and `RAZORPAY_KEY_SECRET` as secret environment variables.
7. Deploy and open the Render URL. The health check is `/api/health`.

`render.yaml` contains the same setup. Use an always-on instance for dependable market-hours streaming; sleeping instances can miss ticks. Kite access tokens usually expire daily, so update `KITE_ACCESS_TOKEN` in Render before the next session.

Set `AUTH_DB_PATH` to a persistent disk path in production. Keep one worker per service, as configured above; SQLite and the KiteTicker are then owned by one process.

This is research context only. It is not an order-entry system or a trading signal.
