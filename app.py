import streamlit as st
import threading
import time
import asyncio
import nest_asyncio
import pandas as pd
from io import BytesIO

# Apply nest_asyncio to allow asyncio to run within the Streamlit thread
nest_asyncio.apply()

# --- Import your core modules (Ensure these files are in your project folder) ---
from ingestion import TickIngestor, SYMBOLS_TO_TRACK
from storage import DataResampler
from analytics import QuantAnalytics
from visualization import plot_price_chart, plot_spread_and_zscore, plot_correlation


# --- Global State and Initialization ---
@st.cache_resource
def initialize_backend():
    """Initializes and runs all necessary backend services only once."""
    print("--- Initializing Backend Services ---")

    # 1. Start Ingestion Worker (Async in a Thread)
    ingestor = TickIngestor(SYMBOLS_TO_TRACK)
    # The lambda function runs asyncio.run() to manage the async loop
    ingestion_thread = threading.Thread(target=lambda: asyncio.run(ingestor.connect_and_run()), daemon=True)
    ingestion_thread.start()

    # 2. Start Resampling Worker (Sync Thread)
    resampler = DataResampler(SYMBOLS_TO_TRACK)
    resampler.start()

    # 3. Initialize Analytics Engine
    analytics_engine = QuantAnalytics()

    return {
        'analytics': analytics_engine,
        'resampler': resampler,
        'ingestion_thread': ingestion_thread
    }


services = initialize_backend()
ANALYTICS = services['analytics']
RESAMPLER = services['resampler']

# --- Streamlit Frontend Layout ---
st.set_page_config(layout="wide", page_title="Real-Time Quant Dashboard", page_icon="📈")

st.title("₿ Live Crypto Pair Trading Analytics Dashboard")
st.markdown("---")

# --- Sidebar Control Panel [cite: 13] ---
with st.sidebar:
    st.header("Trade Pair & Settings")

    symbols_list = SYMBOLS_TO_TRACK

    # Symbol Selection (Multiple products supported)
    asset_y = st.selectbox("Asset 1 (Y)", symbols_list, index=0)
    asset_x = st.selectbox("Asset 2 (X)", symbols_list, index=1)

    # Timeframe and Rolling Window Selection [cite: 13]
    timeframe = st.radio("Timeframe", ['1s', '1m', '5m'], index=1, horizontal=True)
    window = st.slider("Rolling Window Size", 50, 500, 200, help="Number of bars for OLS regression and rolling stats.")

    # Regression Type Selection (Placeholder for extensibility) [cite: 13]
    regression_type = st.selectbox("Regression Type", ["OLS (Ordinary Least Squares)", "Huber (Advanced Extension)"])

    st.markdown("---")

    # ADF Test Trigger [cite: 13]
    if st.button("Trigger ADF Test"):
        st.session_state.run_adf_check = True

    st.subheader("System Status")
    st.caption(f"Ingestion Worker: {'🟢 Running' if services['ingestion_thread'].is_alive() else '🔴 Stopped'}")
    st.caption(f"Resampler Worker: {'🟢 Running' if RESAMPLER.thread.is_alive() else '🔴 Stopped'}")

# --- Main Dashboard ---
unique_run_key = time.time()
# Use st.empty() for placing content that needs near-real-time updates [cite: 8]
placeholder = st.empty()

# The main update loop
metrics = {}
correlation = pd.Series()
with placeholder.container():


        # --- Analytics Calculation ---
        try:
            # Fetch OHLCV data for price chart
            ohlcv_data = ANALYTICS.get_ohlcv_data(asset_y, timeframe, limit=500)

            # Calculate core pair metrics
            metrics = ANALYTICS.calculate_pair_trading_metrics(asset_y, asset_x, timeframe, window)

            # Calculate rolling correlation
            correlation = ANALYTICS.calculate_rolling_correlation(asset_y, asset_x, timeframe, window)

        except Exception as e:
            st.error(f"Data Fetch/Analytics Error: Ensure Redis and DuckDB are running and populated. Error: {e}")
            time.sleep(2)

        # --- Row 1: Summary Stats (Live Update) ---
        st.header("Key Pair Metrics")
        col_r1, col_r2, col_r3, col_r4 = st.columns(4)
        adf_value = metrics.get('adf_p_value', 'N/A')

        # 1. Check if the value is Pandas/NumPy Null OR if it is the string default 'N/A'
        if pd.isna(adf_value) or adf_value == 'N/A':
            # Display the default string value without numeric formatting
            display_adf = "N/A"
        else:
            # Display the numeric value with the required .4f formatting
            display_adf = f"{adf_value:.4f}"

        col_r3.metric("ADF P-Value", display_adf)
        col_r1.metric("Latest Z-Score", f"{metrics.get('latest_z_score', 0.0):.2f}")
        col_r2.metric("Hedge Ratio (β)", f"{metrics.get('hedge_ratio', 'N/A'):.4f}")
        col_r4.metric("Live Correlation", f"{correlation.iloc[-1]:.4f}" if not correlation.empty else "N/A")

        st.markdown("---")

        # --- Row 2: Charts (Spread/Z-Score & Correlation) [cite: 12] ---
        col_v1, col_v2 = st.columns(2)

        with col_v1:
            st.subheader(f"Spread and Z-Score ({timeframe})")
            # Uses the Plotly function from visualization.py
            fig_spread = plot_spread_and_zscore(metrics)
            st.plotly_chart(fig_spread, use_container_width=True,
                key=f"spread_zscore_{asset_y}_{asset_x}_{timeframe}")

        with col_v2:
            st.subheader(f"Rolling Correlation (Window: {window})")
            fig_corr = plot_correlation(correlation)
            st.plotly_chart(fig_corr, use_container_width=True,
                key=f"corr_{asset_y}_{asset_x}_{timeframe}_{window}")

        st.markdown("---")

        # --- Row 3: Price Chart ---
        st.subheader(f"Price Action - {asset_y.upper()}")
        fig_prices = plot_price_chart(ohlcv_data, asset_y, timeframe)
        st.plotly_chart(fig_prices, use_container_width=True,
                key=f"prices_{asset_y}_{timeframe}")

        # --- Row 4: Data Export and Alerts [cite: 9] ---
        st.header("Alerting and Data Management")
        alert_col, data_col, upload_col = st.columns([1.5, 1, 1])

        # Alerting Section [cite: 8]
        with alert_col:
            st.subheader("Rule-Based Alerting")
            # Simple Z-score alert definition [cite: 8]
            z_alert_level = st.number_input("Alert Z-Score >", value=2.5, step=0.1)

            if metrics.get('latest_z_score', 0) > z_alert_level:
                st.error(
                    f"🚨 **ALERT!** {asset_y}/{asset_x} Z-Score reached {metrics['latest_z_score']:.2f} > {z_alert_level}")
            else:
                st.success("System Normal. No active alerts.")

        # Data Export [cite: 9]
        with data_col:
            st.subheader("Data Export")
            # Prepare all processed data for download
            export_df = ANALYTICS.db_conn.execute(f"SELECT * FROM ohlcv WHERE timeframe='{timeframe}'").fetchdf()

            # Convert to CSV in memory
            csv_buffer = BytesIO()
            export_df.to_csv(csv_buffer, index=False)
            csv_bytes = csv_buffer.getvalue()

            st.download_button(
                label=f"Download Processed {timeframe} Data (.csv)",
                data=csv_bytes,
                file_name=f"quant_analytics_{timeframe}_{time.strftime('%Y%m%d')}.csv",
                mime='text/csv'
            )

        # OHLC Upload [cite: 16]
        with upload_col:
            st.subheader("Historical Data Upload")
            st.file_uploader("Upload OHLC CSV Data (Optional)", type=['csv'])
            st.caption("Mandatory functionality, works without dummy upload. [cite: 16]")

    # Control the refresh rate (e.g., update plots every 500ms for near-real-time)