# Real-Time-Crypto-Pair-Trading-Analytics-Dashboard
This project implements a low-latency, real-time data pipeline and dashboard for analyzing cryptocurrency pair trading strategies. It captures live market data, processes it into multiple timeframes, runs econometric models (OLS, ADF Test), and visualizes the results using Streamlit and Plotly.
## 🚀 Key Features
-**Real-Time Data Ingestion:**] Asynchronously connects to the Binance WebSocket (WS) to receive live tick data.
-[**Dual-Layer Storage**:] Uses Redis as a high-speed buffer for immediate tick ingestion and DuckDB as a file-based analytical database for persistent OHLCV data storage.
-[**Multi-Timeframe Resampling:**] Processes raw ticks into OHLCV bars for multiple timeframes (e.g., 1s, 1m, 5m).
-[**Econometric Analysis:**] Performs Ordinary Least Squares (OLS) regression to find the Hedge Ratio (β) and calculates the Augmented Dickey-Fuller (ADF) Test P-Value for cointegration.
-[**Visualization:**] Interactive dashboard built with Streamlit and Plotly to visualize the mean-reverting Spread, Z-Score, and Rolling Correlation.
## 🏛️ System Architecture
