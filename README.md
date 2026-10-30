# Kotak Neo — Options Tick Data Collector

Collects **real-time tick data** for NSE index option chains on expiry days using the [Kotak Neo API](https://github.com/Kotak-Neo/kotak-neo-api) and stores it as per-second CSVs plus 5-minute OHLC candles.

## Supported Indices

| Index | Strike Gap | Instrument Token |
|-------------|------------|------------------|
| NIFTY | 50 | 26000 |
| BANKNIFTY | 100 | 26009 |
| FINNIFTY | 50 | 26037 |
| MIDCPNIFTY | 25 | 26074 |

## Prerequisites

- **Kotak Neo account** with API access (provides consumer key & secret)
- **Python 3.10+**

```bash
pip install neo_api_client pandas python-dateutil nsepython
```

## Configuration

1. Copy [`.env`](.env) and fill in your credentials:

```env
KOTAK_CONSUMER_KEY=your_key_here
KOTAK_CONSUMER_SECRET=your_secret_here
KOTAK_MOBILE_NUMBER=+91XXXXXXXXXX
KOTAK_LOGIN_PASSWORD=your_password
KOTAK_LOGIN_MPIN=your_mpin
BASE_OUTPUT_DIR=C:/Options_Data
```

| Variable | Description |
|---|---|
| `KOTAK_CONSUMER_KEY` | Your Kotak Neo consumer key |
| `KOTAK_CONSUMER_SECRET` | Your Kotak Neo consumer secret |
| `KOTAK_MOBILE_NUMBER` | Registered mobile number (+91 format) |
| `KOTAK_LOGIN_PASSWORD` | Kotak login password |
| `KOTAK_LOGIN_MPIN` | Kotak login MPIN |
| `BASE_OUTPUT_DIR` | Path where data folders are created |

> **⚠️ The `.env` file is gitignored and must never be committed.**

## How It Works

1. **Login** — Authenticates with Kotak Neo via mobile + MPIN
2. **Expiry Detection** — Fetches live expiry dates from NSE using `nsepython` to determine which index is expiring today
3. **ATM Calculation** — Fetches the spot price and rounds to the nearest valid strike
4. **Option Chain** — Builds the full chain (10 OTM + ATM + 10 ITM for both CE & PE = 42 legs)
5. **Tick Collection** — Polls LTP every 0.5s during market hours (09:15–15:30), flushing to CSV every 3 minutes
6. **OHLC Conversion** — After market close, resamples per-second data into 5-minute OHLC candles

## Output Structure

```
Options_Data/
└── Nifty/
    └── 2026-10-30/
        ├── CE/
        │   ├── Per_Second_Data/
        │   │   ├── NIFTY30OCT24500CE.csv
        │   │   └── ...
        │   └── Five_Minute_OHLC/
        │       ├── NIFTY30OCT24500CE.csv
        │       └── ...
        └── PE/
            ├── Per_Second_Data/
            └── Five_Minute_OHLC/
```

## Usage

```bash
python Kotak.py
```

The script runs autonomously — starts before market open, collects data throughout the session, and exits after OHLC conversion.

> **Tip:** For daily unattended runs, use a task scheduler (e.g. cron on Linux, Task Scheduler on Windows) or a dedicated device like a Raspberry Pi.

## Credits

- [Kotak Neo API Team](https://github.com/Kotak-Neo/kotak-neo-api) — for API support and guidance
- [nsepython](https://github.com/unofficed/nsepython) — for reliable NSE data access
