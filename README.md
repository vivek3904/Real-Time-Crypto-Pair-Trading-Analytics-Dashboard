# Real-Time-Crypto-Pair-Trading-Analytics-Dashboard
This project implements a low-latency, real-time data pipeline and dashboard for analyzing cryptocurrency pair trading strategies. It captures live market data, processes it into multiple timeframes, runs econometric models (OLS, ADF Test), and visualizes the results using Streamlit and Plotly.
## 🚀 Key Features
- **Real-Time Data Ingestion:** Asynchronously connects to the Binance WebSocket (WS) to receive live tick data.
- **Dual-Layer Storage**: Uses Redis as a high-speed buffer for immediate tick ingestion and DuckDB as a file-based analytical database for persistent OHLCV data storage.
- **Multi-Timeframe Resampling:** Processes raw ticks into OHLCV bars for multiple timeframes (e.g., 1s, 1m, 5m).
- **Econometric Analysis:** Performs Ordinary Least Squares (OLS) regression to find the Hedge Ratio (β) and calculates the Augmented Dickey-Fuller (ADF) Test P-Value for cointegration.
- **Visualization:** Interactive dashboard built with Streamlit and Plotly to visualize the mean-reverting Spread, Z-Score, and Rolling Correlation.
## 🏛️ System Architecture
The application runs as a cohesive, multi-threaded system orchestrated by app.py.
- **Ingestion Worker (ingestion.py):** Establishes an asynchronous connection (using asyncio and websockets) to the Binance WS and pushes every raw trade tick directly to a Redis list.
- **Resampler Worker (storage.py):** Runs in a separate thread, continuously pulling raw ticks from Redis, resampling them using Pandas into OHLCV bars (1s, 1min, 5min), and persisting them in the DuckDB file (quant_data.db).
- **Analytics Engine (analytics.py):** Reads the resampled OHLCV data from DuckDB, calculates log returns, runs OLS and the ADF test using statsmodels, and generates the Z-Score and Rolling Correlation series.
- **Dashboard (app.py):** Streamlit frontend that fetches the latest results from the Analytics Engine and Plotly charts, updating the display every few seconds.
## **⚙️ Setup and Installation**
- **Python 3.8+**
- **Redis Server:** The Redis server must be running locally on the default port (6379).
    - **WSL/Linux/macOS:** Run redis-server in a separate terminal.
- **Binance API Key:** The ingestion worker connects to the public Binance stream; no API keys are required for this specific implementation.
**1. Clone the Repository**
  ```bash
#!/bin/bash
git clone [https://github.com/vivek3904/Real-Time-Crypto-Pair-Trading-Analytics-Dashboard]
cd [Real-Time Crypto Pair Trading Analytics Dashboard]
