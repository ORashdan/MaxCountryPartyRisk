import pandas as pd
import ccxt
from ccxt.base.errors import BadSymbol

top10 = pd.read_parquet('Top10.parquet')


def get_exchange_data(exchange_called, symbol, side):
    exchange = getattr(ccxt, exchange_called)
    exchange = exchange()
    exchange.load_markets()

    # Determine which symbol format to use
    try:
        symbol_format = f"{symbol}/USDT:USDT"
        fundingRate = exchange.fetch_funding_rate(symbol=symbol_format)
    except BadSymbol:
        try:
            symbol_format = f"{symbol}/USD:USD"
            fundingRate = exchange.fetch_funding_rate(symbol=symbol_format)
        except BadSymbol:
            symbol_format = f"{symbol}/USDC:USDC"
            fundingRate = exchange.fetch_funding_rate(symbol=symbol_format)

    # Get current funding rate and interval
    funding = fundingRate['fundingRate']

    # Get 3-interval mean funding rate
    fundingRateHistory = exchange.fetch_funding_rate_history(symbol=symbol_format, limit=4)
    fr_history_without_latest = fundingRateHistory[:-1]
    df = pd.DataFrame(fr_history_without_latest)
    diffs = df['timestamp'].diff().dropna()
    if len(diffs) == 0:
        interval_hours = None
    else:
        interval_ms = diffs.mode().iloc[0]
        interval_hours = interval_ms/(1000*60*60)
    mean_funding = df['fundingRate'].mean()
    mean_funding = (mean_funding/interval_hours)*8

    # Get average fill price
    ob_data = exchange.fetch_order_book(symbol=symbol_format)
    filled = 0
    spent = 0

    if side == "long":
        asks = ob_data['asks']
        for order in asks:
            price = order[0]
            available_liq = order[1]
            remaining_value = 1000 - spent
            if remaining_value <= 0:
                break
            max_qty_afford = remaining_value / price
            take = min(available_liq, max_qty_afford)
            spent += price * take
            filled += take
    else:
        bids = ob_data['bids']
        for order in bids:
            price = order[0]
            available_liq = order[1]
            remaining_value = 1000 - spent
            if remaining_value <= 0:
                break
            max_qty_afford = remaining_value / price
            take = min(available_liq, max_qty_afford)
            spent += price * take
            filled += take

    avg_price = spent / filled if filled > 0 else 0

    # Get trading fees
    fees = exchange.markets[symbol_format]['taker']
    trading_fees = fees * 1000

    return funding, interval_hours, mean_funding, avg_price, trading_fees


def get_Current_FundingRate(exchange_called,symbol: str): #Returns current funding rate and the interval
    exchange = getattr(ccxt, exchange_called)
    exchange = exchange()
    try:
        fundingRate = exchange.fetch_funding_rate(symbol = f"{symbol}/USDT:USDT")
        funding = fundingRate['fundingRate']
        interval = fundingRate['interval']
    except BadSymbol:
        fundingRate = exchange.fetch_funding_rate(symbol = f"{symbol}/USD:USD")
        funding = fundingRate['fundingRate']
        interval = fundingRate['interval']
    
    return funding, interval 

def get_3Interval_Mean(exchange_called,symbol): #returns the average 3 funding rates (settled)
    exchange = getattr(ccxt, exchange_called)
    exchange= exchange()
    try:
        fundingRate = exchange.fetch_funding_rate_history(symbol = f"{symbol}/USDT:USDT", limit = 4)
        fr_history_without_latest = fundingRate[:-1]
        df = pd.DataFrame(fr_history_without_latest)
        mean_funding = df['fundingRate'].mean()
    except BadSymbol:
        fundingRate = exchange.fetch_funding_rate_history(symbol = f"{symbol}/USD:USD",limit = 4)
        fr_history_without_latest = fundingRate[:-1]
        df = pd.DataFrame(fr_history_without_latest)
        mean_funding = df['fundingRate'].mean()
    return mean_funding

def getAvgFill(exchange_called,symbol,side): #returns the average fill price under the assumption of a 1000$ mkt order
    exchange = getattr(ccxt, exchange_called)
    exchange = exchange()
    filled = 0
    spent = 0
    try:
        ob_data = exchange.fetch_order_book(symbol = f'{symbol}/USDT:USDT')
    except:
        ob_data = exchange.fetch_order_book(symbol = f'{symbol}/USD:USD')
    if side == "long":
        asks = ob_data['asks']
        for price, available_liq in asks:
            remaining_value = 1000 - spent
            if remaining_value<=0:
                break
            max_qty_afford = remaining_value/price
            take = min(available_liq, max_qty_afford)
            spent += price*take
            filled +=take
    else:
        bids = ob_data['bids']
        for price, available_liq in bids:
            remaining_value = 1000 - spent
            if remaining_value<=0:
                break
            max_qty_afford = remaining_value/price
            take = min(available_liq, max_qty_afford)
            spent += price*take
            filled +=take
    avg_price = spent/filled
    return avg_price

def getTradingFees(exchange_called,symbol): #returns the transaction cost assuming 1000$ mkt order
    exchange = getattr(ccxt, exchange_called)
    exchange = exchange()
    exchange.load_markets()
    try:
        fees = exchange.markets[f'{symbol}/USDT:USDT']['taker']
    except:
        fees = exchange.markets[f'{symbol}/USD:USD']['taker']
    fees = fees*1000
    return fees


def process_top10_opportunities():
    """
    Iterates through top10 parquet and collects exchange data for both long and short positions.
    Returns a DataFrame with all the collected data.
    """
    results = []

    for symbol, row in top10.iterrows():
        long_exchange = row['Long_Exchange']
        short_exchange = row['Short_Exchange']

        # Get data for long exchange
        try:
            long_funding, long_interval, long_mean_funding, long_avg_price, long_fees = get_exchange_data(
                long_exchange, symbol, side="long"
            )
            long_data = {
                'funding': long_funding,
                'interval': long_interval,
                'mean_funding': long_mean_funding,
                'avg_price': long_avg_price,
                'fees': long_fees
            }
        except Exception as e:
            print(f"Error getting long data for {symbol} on {long_exchange}: {e}")
            long_data = {
                'funding': None,
                'interval': None,
                'mean_funding': None,
                'avg_price': None,
                'fees': None
            }

        # Get data for short exchange
        try:
            short_funding, short_interval, short_mean_funding, short_avg_price, short_fees = get_exchange_data(
                short_exchange, symbol, side="short"
            )
            short_data = {
                'funding': short_funding,
                'interval': short_interval,
                'mean_funding': short_mean_funding,
                'avg_price': short_avg_price,
                'fees': short_fees
            }
        except Exception as e:
            print(f"Error getting short data for {symbol} on {short_exchange}: {e}")
            short_data = {
                'funding': None,
                'interval': None,
                'mean_funding': None,
                'avg_price': None,
                'fees': None
            }

        # Combine all data
        result = {
            'Symbol': symbol,
            'Long_Exchange': long_exchange,
            'Short_Exchange': short_exchange,
            'Long_Funding': long_data['funding'],
            'Long_Interval': long_data['interval'],
            'Long_Mean_Funding': long_data['mean_funding'],
            'Long_Avg_Price': long_data['avg_price'],
            'Long_Fees': long_data['fees'],
            'Short_Funding': short_data['funding'],
            'Short_Interval': short_data['interval'],
            'Short_Mean_Funding': short_data['mean_funding'],
            'Short_Avg_Price': short_data['avg_price'],
            'Short_Fees': short_data['fees']
        }

        results.append(result)
    df = pd.DataFrame(results)
    df['execution_fees'] = (((df['Short_Avg_Price']-df['Long_Avg_Price'])/df['Long_Avg_Price'])*1000) - (df['Short_Fees']+df['Long_Fees'])
    df['8h_FundingPayment'] = (df['Short_Mean_Funding']*1000) - (df['Long_Mean_Funding']*1000)
    df['Expectancy'] = df['8h_FundingPayment'] + df['execution_fees']
    return df




def fix_intervals():
    df = pd.read_csv('resultsdf.csv')
    df = df.set_index('Symbol')
    for idx, value in df['Long_Interval'].items():
        is_nan = pd.isna(value)
        is_empty_str = isinstance(value, str) and value.strip() == ""
        if is_nan or is_empty_str:
                print(df.loc[idx]['Long_Exchange'])
                new_val = input(f"[{"Long_Interval"}] row {idx} is empty. Enter value: ")
                df.at[idx, "Long_Interval"] = new_val


    for idx, value in df['Short_Interval'].items():
        is_nan = pd.isna(value)
        is_empty_str = isinstance(value, str) and value.strip() == ""
        if is_nan or is_empty_str:
                print(df.loc[idx]['Short_Exchange'])
                new_val = input(f"[{"Short_Interval"}] row {idx} is empty. Enter value: ")
                df.at[idx, "Short_Interval"] = new_val
    return df


df = process_top10_opportunities()
df.to_parquet('data.parquet')

df = pd.read_parquet("data.parquet")
df.to_csv('wtfisgoingon.csv')
