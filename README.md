# Storing-Options-Data-using-Kotak-neo-api

## About

In this python project, we use Kotak Neo Api to fetch live tick data of all NSE Index derivatives(Nifty, Banknifty, Midcapnifty, Finnifty) and store them in CSV format. The choice of using CSV over DBMS was the ease of csv at that time. Looking back, now it makes sense to have used Mysql or some proper database in place of CSV.

## Supported Indices

| Index | Strike Gap | Instrument Token |
|-------------|------------|------------------|
| NIFTY | 50 | 26000 |
| BANKNIFTY | 100 | 26009 |
| FINNIFTY | 50 | 26037 |
| MIDCPNIFTY | 25 | 26074 |

## Prerequisites

- **Kotak Neo account** with API access (consumer key & secret)
- **Python 3.10+**

```bash
pip install neo_api_client pandas python-dateutil nsepython python-dotenv
```

## Configuration

1. Create a `.env` file and fill in your credentials:

```env
KOTAK_CONSUMER_KEY=your_key_here
KOTAK_CONSUMER_SECRET=your_secret_here
KOTAK_MOBILE_NUMBER=+91XXXXXXXXXX
KOTAK_LOGIN_PASSWORD=your_password
KOTAK_LOGIN_MPIN=your_mpin
BASE_OUTPUT_DIR=C:/Options_Data
```

> **⚠️ The `.env` file is gitignored and must never be committed.**

## How It Works

1. **Login** — Authenticates with Kotak Neo via mobile + MPIN
2. **Expiry Detection** — Fetches live expiry dates from NSE using `nsepython` to find which index is expiring today
3. **ATM Calculation** — Fetches spot price and rounds to nearest valid strike
4. **Option Chain** — Builds the chain (10 strikes above and below ATM, both CE & PE)
5. **Tick Collection** — Polls LTP every 0.5s during market hours (09:15–15:30), dumps to CSV every 3 minutes
6. **OHLC Conversion** — After market close, converts per-second data into 5-minute OHLC candles

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

Run before market opens. It will collect data throughout the session and convert to OHLC after market close.

## Credits

- [Kotak Neo API](https://github.com/Kotak-Neo/kotak-neo-api)
- [nsepython](https://github.com/unofficed/nsepython)
