import FinanceDataReader as fdr
import sqlite3
import pandas as pd
from datetime import datetime, timedelta
import os

# 클라우드 실행 환경을 위한 경로 설정 (현재 폴더에 저장)
db_name = "krx_top1000.db"

def calculate_indicators(df):
    df['SMA20'] = df['Close'].rolling(window=20).mean()
    df['SMA60'] = df['Close'].rolling(window=60).mean()
    delta = df['Close'].diff()
    up, down = delta.copy(), delta.copy()
    up[up < 0] = 0
    down[down > 0] = 0
    ema_up = up.ewm(com=13, adjust=False).mean()
    ema_down = down.abs().ewm(com=13, adjust=False).mean()
    df['RSI'] = 100 - (100 / (1 + (ema_up / ema_down)))
    return df

def run_system():
    df_krx = fdr.StockListing('KRX')
    top_1000 = df_krx.sort_values(by='MarCap', ascending=False).head(1000)
    conn = sqlite3.connect(db_name)
    
    for _, row in top_1000.iterrows():
        code, name = row['Code'], row['Name']
        table_name = f"stock_{code}"
        try:
            last_date = pd.read_sql(f"SELECT MAX(Date) FROM {table_name}", conn).iloc[0, 0]
            start_date = (datetime.strptime(last_date, '%Y-%m-%d %H:%M:%S') - timedelta(days=80)).strftime('%Y-%m-%d')
        except:
            last_date = None
            start_date = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')

        df = fdr.DataReader(code, start_date)
        if df.empty: continue
        df = calculate_indicators(df).reset_index()
        if last_date:
            df = df[df['Date'] > last_date]
        if not df.empty:
            df.to_sql(table_name, conn, if_exists='append', index=False)
    conn.close()

if __name__ == "__main__":
    run_system()