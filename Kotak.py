from neo_api_client import NeoAPI
import pandas as pd
import time
from time import sleep
from datetime import datetime
import datetime as dt
from dateutil.relativedelta import relativedelta
import threading
from threading import Thread
import os
import sys
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

consumer_key1='YOUR KOTAK CONSUMER KEY' 
consumer_secret1='YOUR KOTAK CONSUMER SECRET KEY'
mob_no='+91 REGISTERED MOBILE NUMBER'
password1='KOTAK LOGIN PASSWORD'
mpin='KOTAK LOGIN MPIN'



print("Starting Login")
live_data={}

def on_message(message):
    global live_data
    try:
        print(f"Message is: {message}")
        for i in message:
            live_data[i['ts']]=i['ltp']
    except Exception as e:
        print(f"Error is: {e}")
    
def on_error(error_message):
    print(error_message)

client = NeoAPI(consumer_key=consumer_key1, consumer_secret=consumer_secret1, 
                environment='prod', on_message=on_message, on_error=on_error, on_close=None, on_open=None)

# Initiate login by passing any of the combinations mobilenumber & password (or) pan & password (or) userid & password

client.login(mobilenumber=mob_no, password=password1)

# Complete login and generate session token
client.session_2fa(OTP=mpin)
print("Finished Login\nLogged In Successfully!")
trading_instru= instrument_dict['NIFTY']
inst_tokens = [{"instrument_token": trading_instru, "exchange_segment": "nse_cm"}]
temp_price=client.quotes(instrument_tokens=inst_tokens, quote_type="ltp")
print(temp_price)




'''
Instrument tokens/pSymbol for Index of NSE and BSE

BANKNIFTY='26009'
NIFTY='26000'
FINNIFTY='26037'
MIDCPNIFTY='26074'
SENSEX='1'
BANKEX='12'

'''
instrument_dict={'BANKNIFTY':'26009' , 'NIFTY':'26000' , 'FINNIFTY':'26037' , 'MIDCPNIFTY':'26074' , 'SENSEX':'1'}
strike_diff_dict={'nifty_strike_difference': 50,'banknifty_strike_difference': 100,'finnifty_strike_difference': 50,'midcapnifty_strike_difference': 25}





scrp_nfo = client.scrip_master(exchange_segment="NFO")
df = pd.read_csv(scrp_nfo)


#scrp_bfo= client.scrip_master(exchange_segment = "BFO")
#df2=pd.read_csv(scrp_bfo)

date_for_folder = datetime.now().strftime('%Y-%m-%d')
base_directory=''
print("Starting expiry filteration")
def filter_data(df,index1):
    df=df[df['pInstType']=='OPTIDX']
    df=df[df['pSymbolName']==index1]
    df['lExpiryDate ']=df['lExpiryDate '].apply(lambda x: dt.datetime.fromtimestamp(x).date()+relativedelta(years=10))
    exp=df['lExpiryDate '].to_list()
    td=dt.date.today()
    cur_exp=min(exp, key= lambda x: (x - td))
    df=df[df['lExpiryDate ']==cur_exp]
    df['dStrikePrice;']=df['dStrikePrice;'].apply(lambda x: int(str(x)[:5]))
    df=df[['pTrdSymbol','pSymbol','pOptionType','dStrikePrice;','lExpiryDate ']]
    df=df.reset_index(drop=True)
    return df

'''
def filter_data2(df2,index1):
    df2=df2[df2['pInstType']=='OPTIDX']
    df2=df2[df2['pSymbolName']==index1]
    df2['lExpiryDate ']=df2['lExpiryDate '].apply(lambda x: dt.datetime.fromtimestamp(x).date()+relativedelta(years=10))
    exp=df2['lExpiryDate '].to_list()
    td=dt.date.today()
    cur_exp=min(exp, key= lambda x: (x - td))
    df2=df2[df2['lExpiryDate ']==cur_exp]
    df2['dStrikePrice;']=df2['dStrikePrice;'].apply(lambda x: int(str(x)[:5]))
    df2=df2[['pTrdSymbol','pSymbol','pOptionType','dStrikePrice;','lExpiryDate ']]
    df2=df2.reset_index(drop=True)
    return df2
'''


data_Nifty=filter_data(df,'NIFTY')
data_Banknifty=filter_data(df,'BANKNIFTY')
data_Finnifty=filter_data(df,'FINNIFTY')
data_Midcapnifty=filter_data(df,'MIDCPNIFTY')
#data_Sensex=filter_data2(df2,'BSXOPT') (BSE DATES ARE UNREADABLE AND GIBBERISH AND NOT ACCURATE)


#check which index is having expiry today?
today_date = str(dt.date.today())
'''
nifty_exp_date = data_Nifty['lExpiryDate '].unique()
banknifty_exp_date = data_Banknifty['lExpiryDate '].unique()
finnifty_exp_date = data_Finnifty['lExpiryDate '].unique()
midcapnifty_exp_date = data_Midcapnifty['lExpiryDate '].unique()
'''

def fetch_and_print_expiry_dates():
    # Define the function to fetch expiry dates
    def fetch_expiry_dates(index_symbol):
        # Initialize Chrome WebDriver
        service = Service('/path to chromedriver/chromedriver')  # Set the path to chromedriver executable
        driver = webdriver.Chrome(service=service)

        try:
            url = f"https://www.niftytrader.in/nse-option-chain/{index_symbol}"
            driver.get(url)

            # Wait for the select element to be present
            WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CSS_SELECTOR, 'select.form-select')))
            
            # Find the select element containing expiry dates
            expiry_select = driver.find_element(By.CSS_SELECTOR, 'select.form-select')
            expiry_dates = [option.text for option in expiry_select.find_elements(By.TAG_NAME, 'option')]

            return expiry_dates

        except Exception as e:
            print(f"Error fetching data for {index_symbol.upper()}: {e}")
            return None

        finally:
            time.sleep(3)
            driver.quit()

    indices = ["nifty", "banknifty", "finnifty", "midcpnifty"]
    global expiry_dates_dict# Dictionary to store expiry dates for each index
    expiry_dates_dict = {}

    # Fetch expiry dates for each index
    for index_symbol in indices:
        expiry_dates = fetch_expiry_dates(index_symbol)
        expiry_dates_dict[index_symbol] = expiry_dates
    
    # Printing expiry dates for each index
    for index_symbol, expiry_dates in expiry_dates_dict.items():
        print(f"Expiry dates for {index_symbol.upper()}:")
        if expiry_dates:
            for date in expiry_dates:
                print(date)
        else:
            print(f"Failed to fetch expiry dates for {index_symbol.upper()}.")

def run_in_thread():
    thread = threading.Thread(target=fetch_and_print_expiry_dates)
    thread.start()
    thread.join()
    
run_in_thread()


expiry_lists=[]
for x,y in expiry_dates_dict.items():
    a1=y[0].split('/')[::-1]
    expiry_lists.append('-'.join(a1))

print(expiry_lists)




trading_instru=''
index1= None
d= None
print("Starting expiry finder")
if expiry_lists[0]==today_date:
    trading_instru= instrument_dict['NIFTY']
    index1=strike_diff_dict['nifty_strike_difference']
    d= data_Nifty
    base_directory = '/directory to store data/Options_Data/Nifty'
elif expiry_lists[1]==today_date:
    trading_instru= instrument_dict['BANKNIFTY']
    index1=strike_diff_dict['banknifty_strike_difference']
    d= data_Banknifty
    base_directory = '/directory to store data/Options_Data/Banknifty'
elif expiry_lists[2]==today_date:
    trading_instru= instrument_dict['FINNIFTY']
    index1=strike_diff_dict['finnifty_strike_difference']
    d= data_Finnifty
    base_directory = '/directory to store data/Options_Data/Finnifty'
elif expiry_lists[3]==today_date:
    trading_instru= instrument_dict['MIDCPNIFTY']
    index1=strike_diff_dict['midcapnifty_strike_difference']
    d= data_Midcapnifty
    base_directory = '/directory to store data/Options_Data/Midcpnifty'

trading_instru= instrument_dict['NIFTY']
index1=strike_diff_dict['nifty_strike_difference']
d= data_Nifty
base_directory = '/directory to store data/Options_Data/Nifty'
    

print("Finished expiry finder")


'''
for demo nifty work:uncomment this
trading_instru= instrument_dict['NIFTY']
index1=strike_diff_dict['nifty_strike_difference']
d= data_Nifty
base_directory = '\directory to store data\Options_Data\Nifty'
'''




print("Starting tokenization")
inst_tokens = [{"instrument_token": trading_instru, "exchange_segment": "nse_cm"}]
temp_price=client.quotes(instrument_tokens=inst_tokens, quote_type="ltp")
print("finished tokenization")
atm_price=float(temp_price['message'][0]['ltp'])
filtered_atm_strike_price=int(index1*round(atm_price/index1))
print("atm filtered")

#getting tokens of strike price 4 places up and down OTM
ce_option_buy_strike_price4= str(filtered_atm_strike_price+(index1*4))+'CE'
pe_option_buy_strike_price4= str(filtered_atm_strike_price-(index1*4))+'PE'

ce_option_buy_strike_price3= str(filtered_atm_strike_price+(index1*3))+'CE'
pe_option_buy_strike_price3= str(filtered_atm_strike_price-(index1*3))+'PE'

ce_option_buy_strike_price2= str(filtered_atm_strike_price+(index1*2))+'CE'
pe_option_buy_strike_price2= str(filtered_atm_strike_price-(index1*2))+'PE'

ce_option_buy_strike_price1= str(filtered_atm_strike_price+(index1*1))+'CE'
pe_option_buy_strike_price1= str(filtered_atm_strike_price-(index1*1))+'PE'

ce_option_buy_atm_price= str(filtered_atm_strike_price)+'CE'
pe_option_buy_atm_price= str(filtered_atm_strike_price)+'PE'

#ITM NOW:

pe_option_buy_strike_price_1= str(filtered_atm_strike_price+(index1*1))+'PE'
ce_option_buy_strike_price_1= str(filtered_atm_strike_price-(index1*1))+'CE'


pe_option_buy_strike_price_2= str(filtered_atm_strike_price+(index1*2))+'PE'
ce_option_buy_strike_price_2= str(filtered_atm_strike_price-(index1*2))+'CE'


pe_option_buy_strike_price_3= str(filtered_atm_strike_price+(index1*3))+'PE'
ce_option_buy_strike_price_3= str(filtered_atm_strike_price-(index1*3))+'CE'

pe_option_buy_strike_price_4= str(filtered_atm_strike_price+(index1*4))+'PE'
ce_option_buy_strike_price_4= str(filtered_atm_strike_price-(index1*4))+'CE'

print("token generation step1")


ce4_temp_token= None
pe4_temp_token= None
ce3_temp_token= None
pe3_temp_token= None
ce2_temp_token= None
pe2_temp_token= None
ce1_temp_token= None
pe1_temp_token= None
ce_atm_temp_token= None
pe_atm_temp_token= None
ce_4_temp_token= None
pe_4_temp_token= None
ce_3_temp_token= None
pe_3_temp_token= None
ce_2_temp_token= None
pe_2_temp_token= None
ce_1_temp_token= None
pe_1_temp_token= None


ce4_final_token= None
pe4_final_token= None
ce3_final_token= None
pe3_final_token= None
ce2_final_token= None
pe2_final_token= None
ce1_final_token= None
pe1_final_token= None
ce_atm_final_token= None
pe_atm_final_token= None
ce_4_final_token= None
pe_4_final_token= None
ce_3_final_token= None
pe_3_final_token= None
ce_2_final_token= None
pe_2_final_token= None
ce_1_final_token= None
pe_1_final_token= None


for x,y in d['pTrdSymbol'].items():
    a=y[:-8:-1]
    b=a[::-1]
    if b==ce_option_buy_strike_price4:
        ce4_temp_token=x
    elif b==pe_option_buy_strike_price4:
        pe4_temp_token=x
    elif b==ce_option_buy_strike_price3:
        ce3_temp_token=x
    elif b==pe_option_buy_strike_price3:
        pe3_temp_token=x
    elif b==ce_option_buy_strike_price2:
        ce2_temp_token=x
    elif b==pe_option_buy_strike_price2:
        pe2_temp_token=x    
    elif b==ce_option_buy_strike_price1:
        ce1_temp_token=x
    elif b==pe_option_buy_strike_price1:
        pe1_temp_token=x
    elif b==ce_option_buy_atm_price:
        ce_atm_temp_token=x
    elif b==pe_option_buy_atm_price:
        pe_atm_temp_token=x
    elif b==pe_option_buy_strike_price_1:
        pe_1_temp_token=x
    elif b==ce_option_buy_strike_price_1:
        ce_1_temp_token=x
    elif b==pe_option_buy_strike_price_2:
        pe_2_temp_token=x
    elif b==ce_option_buy_strike_price_2:
        ce_2_temp_token=x
    elif b==pe_option_buy_strike_price_3:
        pe_3_temp_token=x
    elif b==ce_option_buy_strike_price_3:
        ce_3_temp_token=x
    elif b==pe_option_buy_strike_price_4:
        pe_4_temp_token=x
    elif b==ce_option_buy_strike_price_4:
        ce_4_temp_token=x



ce4_final_token=d['pSymbol'][ce4_temp_token]
pe4_final_token=d['pSymbol'][pe4_temp_token]
ce3_final_token=d['pSymbol'][ce3_temp_token]
pe3_final_token=d['pSymbol'][pe3_temp_token]
ce2_final_token=d['pSymbol'][ce2_temp_token]
pe2_final_token=d['pSymbol'][pe2_temp_token]
ce1_final_token=d['pSymbol'][ce1_temp_token]
pe1_final_token=d['pSymbol'][pe1_temp_token]
ce_atm_final_token=d['pSymbol'][ce_atm_temp_token]  
pe_atm_final_token=d['pSymbol'][pe_atm_temp_token]
ce_4_final_token=d['pSymbol'][ce_4_temp_token]
pe_4_final_token=d['pSymbol'][pe_4_temp_token]
ce_3_final_token=d['pSymbol'][ce_3_temp_token]
pe_3_final_token=d['pSymbol'][pe_3_temp_token]
ce_2_final_token=d['pSymbol'][ce_2_temp_token]
pe_2_final_token=d['pSymbol'][pe_2_temp_token]
ce_1_final_token=d['pSymbol'][ce_1_temp_token]
pe_1_final_token=d['pSymbol'][pe_1_temp_token]  





#folder creation

new_folder_path = os.path.join(base_directory, date_for_folder)  
os.makedirs(new_folder_path)

ce_folder = os.path.join(new_folder_path, 'CE')  
pe_folder = os.path.join(new_folder_path, 'PE')
os.makedirs(ce_folder)
os.makedirs(pe_folder)

per_second_ce= os.path.join(ce_folder, 'Per_Second_Data')
five_minute_ce= os.path.join(ce_folder, 'Five_Minute_Time_Frame')
os.makedirs(per_second_ce)
os.makedirs(five_minute_ce)

per_second_pe= os.path.join(pe_folder, 'Per_Second_Data')
five_minute_pe= os.path.join(pe_folder, 'Five_Minute_Time_Frame')
os.makedirs(per_second_pe)
os.makedirs(five_minute_pe)



def dump_to_excel(data_list, filename):
    # Convert data_list to a DataFrame
    df = pd.DataFrame(data_list)
    
    try:
        # Try to read the existing Excel file if it exists
        existing_data = pd.read_excel(filename)
        
        # Concatenate existing data with new data
        combined_data = pd.concat([existing_data, df], ignore_index=True)
        
        # Write the combined data to the Excel file
        combined_data.to_excel(filename, index=False)
    except FileNotFoundError:
        # If the file doesn't exist, simply write the new data to the file
        df.to_excel(filename, index=False)






inst2_tokens = [
    {"instrument_token": ce_4_final_token, "exchange_segment": "nse_fo"},
    {"instrument_token": ce_3_final_token, "exchange_segment": "nse_fo"},
    {"instrument_token": ce_2_final_token, "exchange_segment": "nse_fo"},
    {"instrument_token": ce_1_final_token, "exchange_segment": "nse_fo"},
    {"instrument_token": ce_atm_final_token, "exchange_segment": "nse_fo"},
    {"instrument_token": ce1_final_token, "exchange_segment": "nse_fo"},
    {"instrument_token": ce2_final_token, "exchange_segment": "nse_fo"},
    {"instrument_token": ce3_final_token, "exchange_segment": "nse_fo"},
    {"instrument_token": ce4_final_token, "exchange_segment": "nse_fo"},
    {"instrument_token": pe4_final_token, "exchange_segment": "nse_fo"},
    {"instrument_token": pe3_final_token, "exchange_segment": "nse_fo"},
    {"instrument_token": pe2_final_token, "exchange_segment": "nse_fo"},
    {"instrument_token": pe1_final_token, "exchange_segment": "nse_fo"},
    {"instrument_token": pe_atm_final_token, "exchange_segment": "nse_fo"},
    {"instrument_token": pe_1_final_token, "exchange_segment": "nse_fo"},
    {"instrument_token": pe_2_final_token, "exchange_segment": "nse_fo"},
    {"instrument_token": pe_3_final_token, "exchange_segment": "nse_fo"},
    {"instrument_token": pe_4_final_token, "exchange_segment": "nse_fo"}
]


# Initialize empty lists to store LTP records
ltp_pe_1 = []
ltp_pe_2 = []
ltp_pe_3 = []
ltp_pe_4 = []
ltp_pe_atm = []
ltp_pe_5 = []
ltp_pe_6 = []
ltp_pe_7 = []
ltp_pe_8 = []
ltp_ce_1 = []
ltp_ce_2 = []
ltp_ce_3 = []
ltp_ce_4 = []
ltp_ce_atm = []
ltp_ce_5 = []
ltp_ce_6 = []
ltp_ce_7 = []
ltp_ce_8 = []

inst2_tokens_map = {
    ce_4_final_token: ltp_ce_1,
    ce_3_final_token: ltp_ce_2,
    ce_2_final_token: ltp_ce_3,
    ce_1_final_token: ltp_ce_4,
    ce_atm_final_token: ltp_ce_atm,
    ce1_final_token: ltp_ce_5,
    ce2_final_token: ltp_ce_6,
    ce3_final_token: ltp_ce_7,
    ce4_final_token: ltp_ce_8,
    pe4_final_token: ltp_pe_1,
    pe3_final_token: ltp_pe_2,
    pe2_final_token: ltp_pe_3,
    pe1_final_token: ltp_pe_4,
    pe_atm_final_token: ltp_pe_atm,
    pe_1_final_token: ltp_pe_5,
    pe_2_final_token: ltp_pe_6,
    pe_3_final_token: ltp_pe_7,
    pe_4_final_token: ltp_pe_8
}


print("Starting program till market is open...")
# Define market open and close times
market_open = pd.Timestamp('09:14:58')
market_close = pd.Timestamp('15:29:58')

# Time interval for dumping data (in minutes)
dump_interval = 3
quote_data = {'message': [{'trading_symbol': 'temp_placeholder'}]}
# Collect LTP data every second from 9:14 to 3:30
while pd.Timestamp.now() <= market_close:
    # Check if the current time is within the specified range
    if pd.Timestamp.now() >= market_open:
        try:
            # Fetch LTP data (simulated)
            quote_data = client.quotes(instrument_tokens=inst2_tokens, quote_type="ltp")
            
            # Record the LTP data
            current_time = pd.Timestamp.now()
            date_str = current_time.strftime('%Y-%m-%d')  # Format to show only date
            time_str = current_time.strftime('%H:%M:%S')  # Format to show only time
            


            for x in quote_data['message']:
                instrument_tokenx = x['instrument_token']
                instrument_namex = x['trading_symbol']
                ltpx = float(x['ltp'])
                exchange_segment = x['exchange_segment']

                if instrument_tokenx in inst2_tokens_map:
                    inst2_tokens_map[instrument_tokenx].append({'Date': date_str, 'Instrument': instrument_namex, 'Time': time_str, 'LTP': ltpx})
                else:
                    print("No matching token found for instrument_token:", instrument_tokenx)
                
                '''
                print("\n")
                print("ltp_pe_1:", ltp_pe_1)
                print("\n")
                print("ltp_pe_2:", ltp_pe_2)
                print("\n")
                print("ltp_pe_3:", ltp_pe_3)
                print("\n")
                print("ltp_pe_4:", ltp_pe_4)
                print("\n")
                print("ltp_pe_atm:", ltp_pe_atm)
                print("\n")
                print("ltp_pe_5:", ltp_pe_5)
                print("\n")
                print("ltp_pe_6:", ltp_pe_6)
                print("\n")
                print("ltp_pe_7:", ltp_pe_7)
                print("\n")
                print("ltp_pe_8:", ltp_pe_8)
                print("\n")
                print("ltp_ce_1:", ltp_ce_1)
                print("\n")
                print("ltp_ce_2:", ltp_ce_2)
                print("\n")
                print("ltp_ce_3:", ltp_ce_3)
                print("\n")
                print("ltp_ce_4:", ltp_ce_4)
                print("\n")
                print("ltp_ce_atm:", ltp_ce_atm)
                print("\n")
                print("ltp_ce_5:", ltp_ce_5)
                print("\n")
                print("ltp_ce_6:", ltp_ce_6)
                print("\n")
                print("ltp_ce_7:", ltp_ce_7)
                print("\n")
                print("ltp_ce_8:", ltp_ce_8)'''


            # Dump data to Excel files at specified intervals
            #dump_to_excel(ltp_pe_3, path_folder, excel_file_name)
            if current_time.minute % dump_interval == 0 and current_time.second == 0 and current_time.minute != 0:
                for i,x in enumerate(quote_data['message']):
                    instrument_tokenz= x['instrument_token']
                    if instrument_tokenz in inst2_tokens_map:
                        listo=inst2_tokens_map[instrument_tokenz]
                        namo= x['trading_symbol']
                        #change the file path here using trading_symbol
                        dump_to_excel(listo, os.path.join(per_second_ce, namo + '.xlsx'))
                
                print("Dumping Done...")


                # Clear the lists after dumping data to Excel
                print("Clearing Temperory lists for next Cycle...")
                ltp_ce_1.clear() 
                ltp_ce_2.clear()
                ltp_ce_3.clear()
                ltp_ce_4.clear()
                ltp_ce_atm.clear()
                ltp_ce_5.clear()
                ltp_ce_6.clear()
                ltp_ce_7.clear()
                ltp_ce_8.clear()
                ltp_pe_1.clear()
                ltp_pe_2.clear()
                ltp_pe_3.clear()
                ltp_pe_4.clear()
                ltp_pe_atm.clear()
                ltp_pe_5.clear()
                ltp_pe_6.clear()
                ltp_pe_7.clear()
                ltp_pe_8.clear()
                print("Cleared Lists.\n\nBeggining next Cycle...")
            

        except Exception as e:
            print("Error occurred while fetching LTP data:", e)
    
    # Wait for 0.5 second before fetching next LTP data
    sleep(0.5)
    
    
    
print("Market Closed...")    
print("Starting conversion of per second data to 5 minutes time frame...")  
print("Fetching the per second files...")  



# List of folder paths
folder_paths1 = [per_second_pe, per_second_ce]

# Lists to store file paths and filenames
file_paths1 = []
file_names1 = ['']

# Iterate over folders
for folder_path in folder_paths1:
    # Walk through the folder
    for root, dirs, files in os.walk(folder_path):
        # Iterate over files
        for file_name in files:
            # Construct full file path
            file_pathx = os.path.join(root, file_name)
            # Append to the list of file paths
            file_paths1.append(file_pathx)
            # Extract filename from the full file path and append to the list of filenames
            file_names1.append(file_name)

# Display the file paths
print("Files in folders:")
for x, file_path in enumerate(file_paths1, start=1):
    print(f"{x}: {file_path}")

# Display the filenames
print("\nFilenames:")
for x, file_name in enumerate(file_names1, start=1):
    print(f"{x}: {file_name}")




excel_file_pe_1 = file_paths1[0]
excel_file_pe_2 = file_paths1[1]
excel_file_pe_3 = file_paths1[2]
excel_file_pe_4 = file_paths1[3]
excel_file_pe_atm = file_paths1[4]
excel_file_pe_5 = file_paths1[5]
excel_file_pe_6 = file_paths1[6]
excel_file_pe_7 = file_paths1[7]
excel_file_pe_8 = file_paths1[8]


excel_file_ce_1 = file_paths1[9]
excel_file_ce_2 = file_paths1[10]
excel_file_ce_3 = file_paths1[11]
excel_file_ce_4 = file_paths1[12]
excel_file_ce_atm = file_paths1[13]
excel_file_ce_5 = file_paths1[14]
excel_file_ce_6 = file_paths1[15]
excel_file_ce_7 = file_paths1[16]
excel_file_ce_8 = file_paths1[17]

df_pe_1 = pd.read_excel(excel_file_pe_1)
df_pe_2 = pd.read_excel(excel_file_pe_2)
df_pe_3 = pd.read_excel(excel_file_pe_3)
df_pe_4 = pd.read_excel(excel_file_pe_4)
df_pe_atm = pd.read_excel(excel_file_pe_atm)
df_pe_5 = pd.read_excel(excel_file_pe_5)
df_pe_6 = pd.read_excel(excel_file_pe_6)
df_pe_7 = pd.read_excel(excel_file_pe_7)
df_pe_8 = pd.read_excel(excel_file_pe_8)
df_ce_1 = pd.read_excel(excel_file_ce_1)
df_ce_2 = pd.read_excel(excel_file_ce_2)
df_ce_3 = pd.read_excel(excel_file_ce_3)
df_ce_4 = pd.read_excel(excel_file_ce_4)
df_ce_atm = pd.read_excel(excel_file_ce_atm)
df_ce_5 = pd.read_excel(excel_file_ce_5)
df_ce_6 = pd.read_excel(excel_file_ce_6)
df_ce_7 = pd.read_excel(excel_file_ce_7)
df_ce_8 = pd.read_excel(excel_file_ce_8)

ohlc_pe_1 = None


df_lists = [
    '',df_pe_1, df_pe_2, df_pe_3, df_pe_4, df_pe_atm, df_pe_5, df_pe_6, df_pe_7, df_pe_8,
    df_ce_1, df_ce_2, df_ce_3, df_ce_4, df_ce_atm, df_ce_5, df_ce_6, df_ce_7, df_ce_8
]



for i, x in enumerate(quote_data['message'], start=1):
    instrument_namey = x['trading_symbol']
    # Resample LTP data to 5-minute intervals and calculate OHLC
    ohlc_pe_1 = df_lists[i].set_index('Time').resample('5T').agg({'LTP': ['first', 'max', 'min', 'last']})
    ohlc_pe_1.columns = ['Open', 'High', 'Low', 'Close']

    # Add instrument name to OHLC data
    ohlc_pe_1['Instrument'] = instrument_namey 

    # Add time frame column to OHLC data
    ohlc_pe_1['Time Frame'] = ohlc_pe_1.index.strftime('%H:%M') + '-' + (ohlc_pe_1.index + pd.Timedelta(minutes=4, seconds=59)).strftime('%H:%M')

    # Save OHLC data to Excel
    if i <= 9:
        ohlc_pe_1.to_excel(os.path.join(five_minute_pe, file_names1[i]), index=False)
    elif 10 <= i <= 18:
        ohlc_pe_1.to_excel(os.path.join(five_minute_ce, file_names1[i]), index=False)


    
    ohlc_pe_1 =None
    

