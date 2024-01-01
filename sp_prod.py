from confluent_kafka import Producer
import yfinance as yf
import time
import requests
import json

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3',
    'Content-Type': 'application/json',
    'Authorization': 'Bearer <token>'
}


conf = {
    'bootstrap.servers': 'localhost:9092',  
    'client.id': 'stock-price-producer'
}

producer = Producer(conf)

topic = 'conn-events'

ticker_symbol = 'BTC-USD'

def fetch_send_stock_price():
    while True:
        try:
            url = 'https://query2.finance.yahoo.com/v8/finance/chart/btc-usd'

            response = requests.get(url, headers=headers)
            data = json.loads(response.text)
            price = data["chart"]["result"][0]["meta"]["regularMarketPrice"]

            producer.produce(topic, key=ticker_symbol, value=str(price))
            producer.flush()

            print(f"Sent {ticker_symbol} price to Kafka: {price}")

        except Exception as e:
            print(f"Error fetching/sending stock price: {e}")

        time.sleep(30)

fetch_send_stock_price()





