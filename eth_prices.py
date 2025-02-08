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
    item = prices[0]
    print(item)
    db.insert("indx_eth_historical_price", {
        'day': item['date'], 
        'eth_price': float(item['closing_price']),
    })
    db.close()

schedule.every().day.at("13:00").do(fetch_eth_prices)

while True:
    schedule.run_pending()
    time.sleep(1)
