import ccxt
import pandas as pd
import time
import os

def get_data(symbol="BTC/USDT:USDT", timeframe="1h", since="2023-01-01T00:00:00Z"):
    safe_symbol = symbol.replace("/", "_").replace(":", "")
    cache_file = f"data_{safe_symbol}_{timeframe}.parquet"
    
    if os.path.exists(cache_file):
        print(f"Loading from cash: {cache_file}")
        return pd.read_parquet(cache_file)
    
    exchange = ccxt.binance({
        'enableRateLimit': True,
        'options': {'defaultType': 'future'},
    })
    
    print(f"Downloading {symbol} {timeframe} с {since}...")
    data = []
    since_ts = exchange.parse8601(since)
    
    while True:
        ohlcv = exchange.fetch_ohlcv(symbol, timeframe, since=since_ts, limit=1000)
        if len(ohlcv) == 0:
            break
        data.extend(ohlcv)
        since_ts = ohlcv[-1][0] + 1
        time.sleep(0.3)
    
    df = pd.DataFrame(data, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
    df.set_index('timestamp', inplace=True)
    df.to_parquet(cache_file)
    print(f"Saved {len(df)} klines → {cache_file}")
    return df

def load_data(symbol="BTC/USDT:USDT", timeframe="1h", since="2023-01-01T00:00:00Z"):
    return get_data(symbol, timeframe, since)

if __name__ == "__main__":
    load_data()