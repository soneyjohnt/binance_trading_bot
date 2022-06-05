# Importing all the necessary Libraries for The Bot

from email.mime import base
import datetime
from hashlib import new
from threading import Condition
from config import API_SECRET, API_KEY
import csv
from binance import Client, ThreadedWebsocketManager, ThreadedDepthCacheManager
import re
import pandas as pd
client = Client(API_KEY,API_SECRET)
import json
from datetime import datetime, date
import time
from os.path import exists
import shutil

# Getting all  the symbols that are actively traded in the binance spot

prices = client.get_all_tickers()

# for price in prices:
#     print(price)
fieldnames = ["time", "open", "high", "low", "close", "volume", "timestamp"]
kline_intervals = ["5m","15m","30m","1h","12h", "1d"]

tickers=["BTCUSDT", "ETHUSDT", "TRXUSDT", "MANAUSDT", "RENUSDT", "CHRUSDT", "ONTUSDT", "OCEANUSDT", "ARPAUSDT", "BATUSDT", "ADAUSDT", "LRCUSDT", "STORJUSDT", "MATICUSDT", "ENJUSDT", "FTMUSDT", "RUNEUSDT", "SANDUSDT", "UNFIUSDT", "ANTUSDT", "WAVESUSDT", "DOTUSDT", "ATOMUSDT", "ETCUSDT", "LUNAUSDT", "AXSUSDT", "AVAXUSDT","SOLUSDT", "LTCUSDT", "AAVEUSDT", "BNBUSDT", "YFIUSDT", "HNTUSDT", "UNIUSDT"]


# Program for writing a csv file with the required data of the symbols in the list "tickers"
def check_if_file_exists(price, kline_int, foldername):
    filename = foldername +"/" + price+ "_"+ kline_int+".csv"
    file_exists = exists(filename)
    return file_exists, filename


def formatcsv(price, kline_int, filename, end_time_interval, start_time_interval = "1 DEC, 2012"):
    fieldnames = ["time", "open", "high", "low", "close", "volume"]
    candlesticks = []

    klines = client.get_historical_klines(price, kline_int, start_time_interval)

    #print(klines)
    for lines in klines:
        candle_data={}
        candle_data["time"]=datetime.utcfromtimestamp(lines[0]/1000).strftime('%Y-%m-%d %H:%M:%S')
        candle_data["open"]= lines[1]
        candle_data["high"]= lines[2]
        candle_data["low"]= lines[3]
        candle_data["close"]= lines[4]
        candle_data["volume"]= lines[5]
        candle_data["timestamp"]= lines[0]
        candlesticks.append(candle_data)
    # print("################## {} ###################".format(price))
    return candlesticks
            

def check_data_range(price, kline_int, filename, end_date):

    print("Getting the last appended value of the file.....")
    print("..................................................")
    with open(filename, "r") as file:
         last_line = file.readlines()[-1]
    
        #  print(last_line)
    last_line = last_line.split(",")
    
    klines = client.get_historical_klines(price, kline_int, "1 day ago UTC")
  
    
    candletime = last_line[0]
    candletime = candletime.split(" ")
    original_candletime = candletime[0]
    


    month_dict={1:"Jan", 2:"Feb", 3: "Mar", 4:"Apr", 5: "May", 6: "Jun", 7:"Jul", 8:"Aug", 9:"Sep", 10:"Oct", 11: "Nov", 12:"Dec" }
    date_time_obj = str(datetime.strptime(original_candletime, f'%Y-%m-%d'))    
    todays_date = date_time_obj.split("-")
  
    start_date= todays_date[2]+" "+month_dict[int(todays_date[1])]+", "+ todays_date[0]
    
    
    if int(last_line[-1])<klines[-1][0]:
        print("It seems that the file isn't up to date and has some more candlesticks to be updated")
        print("Appending now...")
        candlesticks = formatcsv(price, kline_int, filename, end_date, start_time_interval=start_date)
        k = 0
        # print(candlesticks)
        for i in  candlesticks:

            if i["timestamp"]>int(last_line[-1]):
                with open(filename, 'a', newline = "") as f_object:  
                        
                    dictwriter_object = csv.DictWriter(f_object, fieldnames=fieldnames)
                    dictwriter_object.writerow(i)
    
                    f_object.close()

                k = k+1

            else:
                continue
                
        print("Appended {} values".format(k))
        


    else:
        print("It seems that the values are up to date. Hence, moving on...")


            
    #         date_time_obj = datetime.strptime(candletime, f'%Y-%m-%d %H:%M:%S')
    #         original_date = datetime.strptime(str(date_time_obj.date()), f"%Y-%m-%d").strftime(f'%d-%m-%Y')
    #         print(original_date)
    #         date_text = original_date.split("-")
    #         month_dict={1:"Jan", 2:"Feb", 3: "Mar", 4:"Apr", 5: "May", 6: "Jun", 7:"Jul", 8:"Aug", 9:"Sep", 10:"Oct", 11: "Nov", 12:"Dec" }
    #         new_date = date_text[0]+" "+month_dict[int(date_text[1])]+", "+ date_text[2]
    #         print("New DATE ", new_date)
    #         today = date.today()
    #         todays_date = today.strftime(f"%d-%m-%Y")
    #         print(todays_date)
    #         todays_date = todays_date.split("-")
    #         print(todays_date)
    #         end_date = todays_date[0]+" "+month_dict[int(todays_date[1])]+", "+ todays_date[2]
    #         candlesticks = formatcsv(price, kline_int,"18 Mar, 2022","19 Mar, 2022")
    #         print(candlesticks)  
    #         print(len(candlesticks))
    #     else:
    #         print("no need of that.")
            # k = 0
            # for i in candlesticks:
            #     current_timestamp = float(i["timestamp"])
            #     if current_timestamp>last_timestamp:    
            #         k+=1
            #         with open(filename, 'a') as f_object:  
                        
            #             writer_object = csv.writer(f_object)
            #             writer_object.writerow(i)
            
            #             f_object.close()

            #     else:
            #         print("The data is upto date till ", datetime.utcfromtimestamp(klines[-1][0]/1000).strftime(f'%Y-%m-%d %H:%M:%S'))
            # print(k)

            
def writecsv(filename, candlesticks):
        
    with open(filename, 'w', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(candlesticks)



if __name__ == "__main__":
    while True:
        start = 0.0
        end = 0.0
        start = time.perf_counter()
        print("Hey. Let's start building the data from the  Binance API...")
        print("We'll be collecting the data from pairs listed below")
        for i in tickers:
            print(i)
        month_dict={1:"Jan", 2:"Feb", 3: "Mar", 4:"Apr", 5: "May", 6: "Jun", 7:"Jul", 8:"Aug", 9:"Sep", 10:"Oct", 11: "Nov", 12:"Dec" }
        today = date.today()
        todays_date = today.strftime(f"%d-%m-%Y")
        # print(todays_date)
        todays_date = todays_date.split("-")
        # print(todays_date)
        print("We'll be collecting data till the last updated candlestick in the Binance API")
        end_date = todays_date[0]+" "+month_dict[int(todays_date[1])]+", "+ todays_date[2]
        for  price in tickers:
            for kline_int in kline_intervals:
                print("Collecting {} {} data from Binance API..... This may take some time.".format(price, kline_int))
                
                file_exists = check_if_file_exists(price, kline_int, foldername = "data")

                filename = file_exists[1]
                if file_exists[0] is True:
                    print("It seems that file named {} already exists in the folder 'data'..".format(filename))
                
                    check_data_range(price, kline_int, filename, end_date)
                else:
                    
                    candlesticks = formatcsv(price, kline_int, filename, end_time_interval=end_date)
                    writecsv(filename, candlesticks)
                print("The  data is all set. Now.. let's pop off the timestamp and make a copy for the use of backtrader module.")
                data = pd.read_csv(filename, index_col=0)
                
                data = data.drop("timestamp", axis = 1)
                
                file_exists = check_if_file_exists(price, kline_int, foldername = "backtraderdata")
                filename = file_exists[1]
                
                data.to_csv(filename)
        end = time.perf_counter()
        print("Time taken for completion", end-start)
        time.sleep(2)
                # src_path = filename
                # dst_path = "backtrader/"+filename
                # shutil.move(src_path, dst_path)


            


