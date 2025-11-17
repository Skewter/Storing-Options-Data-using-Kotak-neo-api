This is a python project that stores daily tick data of nse index derivatives which includes nifty, banknifty, finnifty, micapnifty. The tick data is collected using kotak neo api.
To use this python code, you need:
a)Kotak neo accout
b)Kotak neo trade api access(which will give you consumer token and secret key for connecting to their api service)




Working:
The code first logs you in. Then it fetches which expiry is today?
It then collects tick data and stores it in list format. 
Then it empties the list in multiple cycles into a csv file.
this emptying of data in multiple cycles is carried out from the start of the market at 9:15 till the close of market till 3:30.
When markets close, the program automatically shuts itself and the process is over.
This project was ran for a period of 1 month till the project was live. During this time, a raspberry pi 5 device was used 24*7  to run this code daily.
As time shall pass, this project might become irrelevant due to changes in api calls and used methods.
Credits: Kotak Neo Api team(for guiding us at points where we felt stuck) 
https://github.com/Kotak-Neo/kotak-neo-api
