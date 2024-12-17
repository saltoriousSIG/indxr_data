ufrom web3 import Web3
import os
import json
import dotenv
import pymongo
import time
import schedule
import requests
from ratelimit import limits, sleep_and_retry

dotenv.load_dotenv()

w3 = Web3(Web3.HTTPProvider(os.getenv("PROVIDER_URL")))
address = os.getenv("ADDRESS")
baseURL = "https://base.blockscout.com/api/v2"

dbclient = pymongo.MongoClient(os.getenv("DB_URL"))
dbsp = dbclient[os.getenv("DB_COLLECTION")]
indexdb = dbsp[os.getenv("DB_TABLE")]

with open('abi/indxr.json', 'r') as file:
    elixrABI = json.load(file)

with open('abi/datastore.json','r') as file:
    datastoreABI = json.load(file)

datastore = w3.eth.contract(address=address, abi=datastoreABI)

@sleep_and_retry
@limits(calls=5, period=1)
def get_index_addresses():
    return datastore.functions.getIndexes().call()

@sleep_and_retry
@limits(calls=5, period=1)
def get_tokens(indexobj):
    return indexobj.functions.fetch_index().call()[0]

def fetch_prices():
    """get all indexes from main smart contract"""
    for indexadr in get_index_addresses():
        """create index obj"""
        indexobj = w3.eth.contract(address=indexadr ,abi=elixrABI)

        total = 0
        prices = []

        """get all tokens in indexobj"""
        tokens = get_tokens(indexobj)
        for token in tokens:
            contract_address = token[0]
            balance = token[2] / 10 ** 18

            response = requests.get(f"{baseURL}/tokens/{contract_address}")
            if response.status_code == 200:
                tokenprice = float(response.json()['exchange_rate']) * balance

                prices.append({contract_address:tokenprice})
                total += tokenprice
            else:
                print(f"Error with getting price for {contract_address}: {response.status_code} - {response.text}")

        indexdb.insert_one({"address":indexadr,"total":total, "breakdown":prices, "timestamp":int(time.time())})
    print("Updated: "+time.strftime("%Y-%m-%d %H:%M:%S"))

schedule.every(5).minutes.do(fetch_prices)
#schedule.every(20).seconds.do(fetch_prices)


while True:
    schedule.run_pending()
    time.sleep(1)