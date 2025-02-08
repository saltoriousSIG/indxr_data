from timescale_connector import TimescaleConnection
import requests
import schedule
import os
import dotenv
import time

dotenv.load_dotenv()


def fetch_eth_prices():
    db = TimescaleConnection(os.getenv("DB_HOST_NEW"), os.getenv("DB_PORT_NEW"), os.getenv("DB_NAME_NEW"), os.getenv("DB_USER_NEW"), os.getenv("DB_PASS_NEW"))
    db.connect()
    url = "https://base.blockscout.com/api/v2/stats/charts/market"
    response = requests.get(url)
    data = response.json()
    prices = data['chart_data']
    for p in prices: 
        db.insert("indx_eth_historical_price", {
            'day': p['date'], 
            'eth_price': float(p['closing_price']),
        })
    db.close()


fetch_eth_prices()
