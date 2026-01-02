import requests
import pandas as pd
import json
import numpy as np

# supported_exchanges = ['binance', 'bingx', 'bitget', 'bybit', 'cryptocom', 'gateio', 'hibachi', 'hyperliquid', 'htx', 'mexc', 'okx']
supported_exchanges = ['bingx', 'bitget', 'bybit', 'cryptocom', 'gateio', 'hibachi', 'hyperliquid', 'huobi', 'mexc', 'okx']
#htx = huobi

# Get funding rate data and save to JSON file
response = requests.get('https://api.loris.tools/funding')
response_json = response.json()
fr_data = response_json["funding_rates"]

with open('data.json', 'w') as f:
    json.dump(fr_data, f)

loris_df = pd.read_json('data.json')
loris_df = loris_df[supported_exchanges]

#Find max arb, note that these are normalised into 8hr 
max_arb = loris_df.max(axis=1) - loris_df.min(axis=1)
long_exchange = loris_df.idxmin(axis=1)
short_exchange = loris_df.idxmax(axis=1)

loris_df['Max_arb'] = max_arb
loris_df['Long_Exchange'] = long_exchange
loris_df['Short_Exchange'] = short_exchange

loris_df.sort_values(by="Max_arb", ascending= False, inplace=True)
top10 = loris_df.nlargest(10,'Max_arb')

print(top10)

top10.to_parquet('Top10.parquet')

