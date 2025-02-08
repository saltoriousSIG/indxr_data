from web3 import Web3
from ratelimit import limits, sleep_and_retry
from timescale_connector import TimescaleConnection
from datetime import datetime
import os
import json
import pytz
import dotenv
import time
import schedule
import requests
dotenv.load_dotenv()

#TODO: Batch calls for if there are a large number of indexes

# constants
diamond_address = os.getenv("DIAMOND_FACET_ADDRESS")
with open('abi/indxr_data_facet.json', 'r') as file:
    indxr_data_abi = json.load(file)
with open('abi/global_datastore_facet.json','r') as file:
    global_data_abi = json.load(file)
with open('abi/erc20_abi.json','r') as file:
    erc20_abi = json.load(file)

#connectors
w3 = Web3(Web3.HTTPProvider(os.getenv("RPC_URL")))
global_data = w3.eth.contract(address=diamond_address, abi=global_data_abi)

def get_index_addresses():
    return global_data.functions.get_all_indexes().call()

def get_tokens(index_address):
    indx = w3.eth.contract(address=index_address, abi=indxr_data_abi)
    return indx.functions.fetch_indx_tokens().call()

@sleep_and_retry
@limits(calls=5, period=1)
def fetch_blockscout_data(address): 
    try:
        baseURL = "https://base.blockscout.com/api/v2"
        response = requests.get(f"{baseURL}/tokens/{address}")
        response.raise_for_status() 
        return response.json()
    except HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
        return None
    except RequestException as req_err:
        print(f"Request error occurred: {req_err}")
        return None
    except JSONDecodeError as json_err:
        print(f"JSON decode error: {json_err}")
        return None
    except Exception as err:
        print(f"An unexpected error occurred: {err}")
        return None
    return None

def fetch_prices():
    db = TimescaleConnection(os.getenv("DB_HOST_NEW"), os.getenv("DB_PORT_NEW"), os.getenv("DB_NAME_NEW"), os.getenv("DB_USER_NEW"), os.getenv("DB_PASS_NEW"))
    db.connect()
    """get all indexes from main smart contract"""
    for index_address in get_index_addresses():
        """create index obj"""
        total_value = 0
        #get all tokens in indexobj
        tokens = get_tokens(index_address)
        for token in tokens:
            print(token)
            print(token[2])
            tkn,_,bal,_,_ = token
            token_address = tkn
            token = w3.eth.contract(address=token_address, abi=erc20_abi)
            price_data = fetch_blockscout_data(token_address)
            decimals = token.functions.decimals().call()
            print(decimals)
            balance = bal / 10 ** decimals 
            if bool(price_data):
                tokenprice = float(price_data['exchange_rate']) * balance
                total_value += tokenprice
            else:
                print(f"Error with getting price for {contract_address}: {response.status_code} - {response.text}")
        #add to timescaledb 
        # Convert integer timestamp to datetime
        timestamp = pytz.utc.localize(datetime.fromtimestamp(int(time.time())))
        db.insert("indx_prices_hyper",{
            'time': timestamp, 
            'indx_address': index_address,
            'price': total_value
        })
    db.close()

schedule.every(1).minutes.do(fetch_prices)

while True:
    schedule.run_pending()
    time.sleep(1)
