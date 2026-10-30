# Kotak Neo Options Tick Data Collector
# collects real time tick data for nse index options on expiry days
# stores per second data as csv and converts to 5 min OHLC after market close
# pip install neo_api_client pandas python-dateutil nsepython python-dotenv

from neo_api_client import NeoAPI
import pandas as pd
import time
from time import sleep
from datetime import datetime
import datetime as dt
from dateutil.relativedelta import relativedelta
import os
import sys
import csv
from dotenv import load_dotenv
from nsepython import nse_optionchain_scrapper


# loading credentials from .env file (keep your secrets safe!)
load_dotenv()

consumer_key1 = os.environ.get('KOTAK_CONSUMER_KEY', '')
consumer_secret1 = os.environ.get('KOTAK_CONSUMER_SECRET', '')
mob_no = os.environ.get('KOTAK_MOBILE_NUMBER', '')
password1 = os.environ.get('KOTAK_LOGIN_PASSWORD', '')
mpin = os.environ.get('KOTAK_LOGIN_MPIN', '')
base_output_dir = os.environ.get('BASE_OUTPUT_DIR', '')

# check if all credentials are filled in
if not all([consumer_key1, consumer_secret1, mob_no, password1, mpin, base_output_dir]):
    print("ERROR: Some credentials are missing! Please fill in your .env file")
    sys.exit(1)


'''
Instrument tokens/pSymbol for Index of NSE

BANKNIFTY='26009'
NIFTY='26000'
FINNIFTY='26037'
MIDCPNIFTY='26074'
'''
instrument_dict = {'BANKNIFTY': '26009', 'NIFTY': '26000', 'FINNIFTY': '26037', 'MIDCPNIFTY': '26074'}
strike_diff_dict = {
    'nifty_strike_difference': 50,
    'banknifty_strike_difference': 100,
    'finnifty_strike_difference': 50,
    'midcapnifty_strike_difference': 25
}

# how many strikes above and below ATM to track
num_strikes = 10

# how often to dump data to csv (in minutes)
dump_interval = 3


# ======================== LOGIN ========================

print("Starting Login")
live_data = {}

def on_message(message):
    global live_data
    try:
        for i in message:
            live_data[i['ts']] = i['ltp']
    except Exception as e:
        print(f"Error in message: {e}")

def on_error(error_message):
    print(f"Error: {error_message}")

client = NeoAPI(consumer_key=consumer_key1, consumer_secret=consumer_secret1,
                environment='prod', on_message=on_message, on_error=on_error, on_close=None, on_open=None)

client.login(mobilenumber=mob_no, password=password1)
client.session_2fa(OTP=mpin)
print("Finished Login\nLogged In Successfully!")


# ======================== SCRIP MASTER ========================

print("Downloading scrip master...")
scrp_nfo = client.scrip_master(exchange_segment="NFO")
df = pd.read_csv(scrp_nfo)
print(f"Scrip master loaded - {len(df)} rows")


# ======================== FILTER EXPIRY DATA ========================

print("Starting expiry filteration")

def filter_data(df, index1):
    # filter only option index instruments for the given index
    df = df[df['pInstType'] == 'OPTIDX']
    df = df[df['pSymbolName'] == index1]

    # kotak dates need +10 years offset (weird but thats how it works)
    df['lExpiryDate '] = df['lExpiryDate '].apply(
        lambda x: dt.datetime.fromtimestamp(x).date() + relativedelta(years=10)
    )

    # find the nearest expiry date
    exp = df['lExpiryDate '].to_list()
    td = dt.date.today()
    future_exp = [d for d in exp if d >= td]
    if not future_exp:
        print(f"No future expiry found for {index1}!")
        return pd.DataFrame()
    cur_exp = min(future_exp)
    df = df[df['lExpiryDate '] == cur_exp]

    # clean strike price
    df['dStrikePrice;'] = df['dStrikePrice;'].apply(lambda x: int(str(x)[:5]))
    df = df[['pTrdSymbol', 'pSymbol', 'pOptionType', 'dStrikePrice;', 'lExpiryDate ']]
    df = df.reset_index(drop=True)
    return df


data_Nifty = filter_data(df, 'NIFTY')
data_Banknifty = filter_data(df, 'BANKNIFTY')
data_Finnifty = filter_data(df, 'FINNIFTY')
data_Midcapnifty = filter_data(df, 'MIDCPNIFTY')
print("Finished expiry filteration")


# ======================== FETCH EXPIRY DATES FROM NSE ========================
# using nsepython to get live expiry dates from NSE website
# (earlier we used selenium + niftytrader.in but that website stopped working)

print("Fetching expiry dates from NSE...")

nse_symbol_map = {
    'NIFTY': 'NIFTY',
    'BANKNIFTY': 'BANKNIFTY',
    'FINNIFTY': 'FINNIFTY',
    'MIDCPNIFTY': 'MIDCPNIFTY'
}

expiry_dates_dict = {}
for index_name, nse_symbol in nse_symbol_map.items():
    try:
        data = nse_optionchain_scrapper(nse_symbol)
        dates = data.get('records', {}).get('expiryDates', [])
        expiry_dates_dict[index_name] = dates
        print(f"Expiry dates for {index_name}: {dates[:3]}")
    except Exception as e:
        print(f"Failed to fetch expiry for {index_name}: {e}")
        expiry_dates_dict[index_name] = []


# ======================== CHECK WHICH INDEX HAS EXPIRY TODAY ========================

print("Starting expiry finder")
today_date = str(dt.date.today())

trading_instru = ''
index1 = None
d = None
date_for_folder = datetime.now().strftime('%Y-%m-%d')
base_directory = ''

# check each index - is its nearest expiry today?
for index_name, dates in expiry_dates_dict.items():
    if not dates:
        continue
    try:
        # NSE gives dates like "31-Oct-2026"
        nearest_exp = datetime.strptime(dates[0].strip(), "%d-%b-%Y").date()
        if str(nearest_exp) == today_date:
            if index_name == 'NIFTY':
                trading_instru = instrument_dict['NIFTY']
                index1 = strike_diff_dict['nifty_strike_difference']
                d = data_Nifty
                base_directory = os.path.join(base_output_dir, 'Nifty')
            elif index_name == 'BANKNIFTY':
                trading_instru = instrument_dict['BANKNIFTY']
                index1 = strike_diff_dict['banknifty_strike_difference']
                d = data_Banknifty
                base_directory = os.path.join(base_output_dir, 'Banknifty')
            elif index_name == 'FINNIFTY':
                trading_instru = instrument_dict['FINNIFTY']
                index1 = strike_diff_dict['finnifty_strike_difference']
                d = data_Finnifty
                base_directory = os.path.join(base_output_dir, 'Finnifty')
            elif index_name == 'MIDCPNIFTY':
                trading_instru = instrument_dict['MIDCPNIFTY']
                index1 = strike_diff_dict['midcapnifty_strike_difference']
                d = data_Midcapnifty
                base_directory = os.path.join(base_output_dir, 'Midcpnifty')
            print(f"Today's expiry: {index_name}")
            break
    except Exception as e:
        print(f"Could not parse date for {index_name}: {e}")

# if no expiry found today, default to nifty (for testing/demo)
if not trading_instru:
    print("No expiry today - defaulting to NIFTY")
    trading_instru = instrument_dict['NIFTY']
    index1 = strike_diff_dict['nifty_strike_difference']
    d = data_Nifty
    base_directory = os.path.join(base_output_dir, 'Nifty')

print("Finished expiry finder")


# ======================== GET ATM STRIKE PRICE ========================

print("Starting tokenization")
inst_tokens = [{"instrument_token": trading_instru, "exchange_segment": "nse_cm"}]
temp_price = client.quotes(instrument_tokens=inst_tokens, quote_type="ltp")
print(f"Spot price data: {temp_price}")

atm_price = float(temp_price['message'][0]['ltp'])
filtered_atm_strike_price = int(index1 * round(atm_price / index1))
print(f"ATM strike: {filtered_atm_strike_price}")
print("Finished tokenization")


# ======================== BUILD OPTION CHAIN ========================
# getting tokens for strikes above and below ATM
# num_strikes up and down for both CE and PE

print(f"Building option chain - {num_strikes} strikes each side of ATM")

option_chain = []  # list to store all the option legs we want to track

for offset in range(-num_strikes, num_strikes + 1):
    strike = filtered_atm_strike_price + (offset * index1)

    for opt_type in ['CE', 'PE']:
        # search in the filtered scrip data for this strike and option type
        match = d[(d['dStrikePrice;'] == strike) & (d['pOptionType'] == opt_type)]

        if match.empty:
            print(f"Warning: no scrip found for {strike}{opt_type} - skipping")
            continue

        row = match.iloc[0]
        option_chain.append({
            'strike': strike,
            'option_type': opt_type,
            'offset': offset,
            'instrument_token': str(row['pSymbol']),
            'trading_symbol': row['pTrdSymbol']
        })

print(f"Option chain built - {len(option_chain)} legs total")


# ======================== FOLDER CREATION ========================

new_folder_path = os.path.join(base_directory, date_for_folder)
os.makedirs(new_folder_path, exist_ok=True)

ce_folder = os.path.join(new_folder_path, 'CE')
pe_folder = os.path.join(new_folder_path, 'PE')
os.makedirs(ce_folder, exist_ok=True)
os.makedirs(pe_folder, exist_ok=True)

per_second_ce = os.path.join(ce_folder, 'Per_Second_Data')
five_minute_ce = os.path.join(ce_folder, 'Five_Minute_OHLC')
os.makedirs(per_second_ce, exist_ok=True)
os.makedirs(five_minute_ce, exist_ok=True)

per_second_pe = os.path.join(pe_folder, 'Per_Second_Data')
five_minute_pe = os.path.join(pe_folder, 'Five_Minute_OHLC')
os.makedirs(per_second_pe, exist_ok=True)
os.makedirs(five_minute_pe, exist_ok=True)

print(f"Folders created at: {new_folder_path}")


# ======================== CSV DUMP FUNCTION ========================

def dump_to_csv(data_list, filename):
    # writes tick data to a csv file (appends if file exists)
    if not data_list:
        return
    file_exists = os.path.exists(filename) and os.path.getsize(filename) > 0
    with open(filename, 'a', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=['Date', 'Instrument', 'Time', 'LTP'])
        if not file_exists:
            writer.writeheader()
        writer.writerows(data_list)


# ======================== PREPARE FOR DATA COLLECTION ========================

# build the instrument token list for the quotes API
inst2_tokens = []
for leg in option_chain:
    inst2_tokens.append({"instrument_token": leg['instrument_token'], "exchange_segment": "nse_fo"})

# create a buffer (list) for each option leg to store ticks temporarily
tick_buffers = {}
for leg in option_chain:
    tick_buffers[leg['instrument_token']] = []

# map each token to its option type so we know which folder to save to
token_info = {}
for leg in option_chain:
    token_info[leg['instrument_token']] = leg['option_type']


# ======================== MAIN DATA COLLECTION LOOP ========================

print("Starting program till market is open...")
market_open = pd.Timestamp('09:14:58')
market_close = pd.Timestamp('15:29:58')

# collect LTP data every ~0.5 seconds from 9:15 to 3:30
while pd.Timestamp.now() <= market_close:
    if pd.Timestamp.now() >= market_open:
        try:
            # fetch LTP for all our option legs
            quote_data = client.quotes(instrument_tokens=inst2_tokens, quote_type="ltp")

            current_time = pd.Timestamp.now()
            date_str = current_time.strftime('%Y-%m-%d')
            time_str = current_time.strftime('%H:%M:%S')

            # store each tick in its buffer
            for x in quote_data['message']:
                token = x['instrument_token']
                if token in tick_buffers:
                    tick_buffers[token].append({
                        'Date': date_str,
                        'Instrument': x['trading_symbol'],
                        'Time': time_str,
                        'LTP': float(x['ltp'])
                    })
                else:
                    print("No matching token found for:", token)

            # dump data to csv files at the set interval
            if current_time.minute % dump_interval == 0 and current_time.second == 0 and current_time.minute != 0:
                print("Dumping data to CSV...")
                for token, ticks in tick_buffers.items():
                    if not ticks:
                        continue
                    symbol = ticks[0]['Instrument']
                    opt_type = token_info[token]
                    if opt_type == 'CE':
                        filepath = os.path.join(per_second_ce, symbol + '.csv')
                    else:
                        filepath = os.path.join(per_second_pe, symbol + '.csv')
                    dump_to_csv(ticks, filepath)

                print("Dumping Done...")

                # clear the buffers for next cycle
                print("Clearing Temperory lists for next Cycle...")
                for token in tick_buffers:
                    tick_buffers[token].clear()
                print("Cleared Lists.\n\nBeggining next Cycle...")

        except Exception as e:
            print(f"Error occurred while fetching LTP data: {e}")

    # wait before next poll
    sleep(0.5)


# ======================== MARKET CLOSED ========================

print("Market Closed...")
print("Starting conversion of per second data to 5 minutes time frame...")

# final dump of any remaining data in buffers
print("Final dump of remaining data...")
for token, ticks in tick_buffers.items():
    if not ticks:
        continue
    symbol = ticks[0]['Instrument']
    opt_type = token_info[token]
    if opt_type == 'CE':
        filepath = os.path.join(per_second_ce, symbol + '.csv')
    else:
        filepath = os.path.join(per_second_pe, symbol + '.csv')
    dump_to_csv(ticks, filepath)
print("Final dump complete")


# ======================== CONVERT TO 5 MINUTE OHLC ========================

print("Fetching the per second files...")

# go through both CE and PE per second folders
folder_pairs = [
    (per_second_ce, five_minute_ce),
    (per_second_pe, five_minute_pe)
]

for second_folder, ohlc_folder in folder_pairs:
    if not os.path.exists(second_folder):
        continue

    for file_name in os.listdir(second_folder):
        if not file_name.endswith('.csv'):
            continue

        input_path = os.path.join(second_folder, file_name)
        output_path = os.path.join(ohlc_folder, file_name)

        try:
            df_temp = pd.read_csv(input_path)
            if df_temp.empty:
                continue

            # convert time column and resample to 5 min OHLC
            df_temp['Time'] = pd.to_datetime(df_temp['Time'], format='%H:%M:%S')
            df_temp = df_temp.set_index('Time')

            ohlc = df_temp['LTP'].resample('5min').agg({
                'Open': 'first',
                'High': 'max',
                'Low': 'min',
                'Close': 'last'
            }).dropna()

            # add instrument name and time frame columns
            ohlc['Instrument'] = df_temp['Instrument'].iloc[0]
            ohlc['Time Frame'] = (
                ohlc.index.strftime('%H:%M') + '-' +
                (ohlc.index + pd.Timedelta(minutes=4, seconds=59)).strftime('%H:%M')
            )

            ohlc.to_csv(output_path, index=False)
            print(f"Converted: {file_name}")

        except Exception as e:
            print(f"Error converting {file_name}: {e}")

print("Conversion complete!")
print(f"\nAll done. Data stored at: {new_folder_path}")
