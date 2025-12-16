import pandas as pd
import duckdb
import statsmodels.api as sm
from statsmodels.tsa.stattools import adfuller
from typing import Dict, Any, List
import numpy as np

# --- Configuration ---
DUCKDB_FILE = "quant_data.db"


class QuantAnalytics:
    """
    Handles all quantitative computations, reading data from DuckDB.
    """

    def __init__(self):
        # Establish the connection to the persistent DuckDB file
        self.db_conn = duckdb.connect(database=DUCKDB_FILE)
        print(f"QuantAnalytics connected to DuckDB at {DUCKDB_FILE}")

    def get_ohlcv_data(self, symbol: str, timeframe: str, limit: int) -> pd.DataFrame:
        """
        Fetches the latest OHLCV data for a given symbol and timeframe from DuckDB.
        """
        query = f"""
            SELECT time, open, high, low, close, volume 
            FROM ohlcv 
            WHERE symbol = '{symbol.lower()}' AND timeframe = '{timeframe}'
            ORDER BY time DESC 
            LIMIT {limit}
        """
        df = self.db_conn.execute(query).fetchdf()

        if df.empty:
            return pd.DataFrame()

        # Set 'time' as index and sort ascending for proper time-series analysis
        df['time'] = pd.to_datetime(df['time'])
        df = df.set_index('time').sort_index()
        return df

    def calculate_rolling_correlation(self, asset1: str, asset2: str, timeframe: str, window: int) -> pd.Series:
        """
        Calculates the rolling correlation of log returns between two assets.
        """
        # Fetch data for both assets
        df1 = self.get_ohlcv_data(asset1, timeframe, limit=window + 10)  # Fetch a little extra
        df2 = self.get_ohlcv_data(asset2, timeframe, limit=window + 10)

        if df1.empty or df2.empty:
            return pd.Series(dtype='float64')

        # Combine prices and calculate returns
        combined_df = pd.DataFrame({
            'P1': df1['close'],
            'P2': df2['close']
        }).dropna()

        # Calculate log returns
        R1 = combined_df['P1'].apply(np.log).diff().dropna()
        R2 = combined_df['P2'].apply(np.log).diff().dropna()

        # Combine returns and calculate rolling correlation
        returns_df = pd.DataFrame({'R1': R1, 'R2': R2}).dropna()

        # Calculate the rolling correlation over the specified window
        rolling_corr = returns_df['R1'].rolling(window=window).corr(returns_df['R2'])

        return rolling_corr.tail(window)

    def calculate_pair_trading_metrics(self, asset1: str, asset2: str, timeframe: str, window: int) -> Dict[str, Any]:
        """
        Calculates Hedge Ratio (OLS), Spread, Z-Score, and ADF Test statistics.

        Args:
            asset1 (Y variable), asset2 (X variable)

        Returns:
            A dictionary containing all results and series for plotting.
        """
        # Fetch enough data for the rolling window calculation
        limit = window * 2 + 10  # Get enough points to ensure stable calculation
        df1 = self.get_ohlcv_data(asset1, timeframe, limit)
        df2 = self.get_ohlcv_data(asset2, timeframe, limit)

        if df1.empty or df2.empty or len(df1) < window or len(df2) < window:
            return {'error': 'Insufficient data for analysis.',
                    'spread_series': pd.Series(),
                    'z_score_series': pd.Series(),
                    'hedge_ratio': pd.NA,
                    'adf_p_value': pd.NA,
                    'latest_z_score': 0.0,
                    'latest_spread': 0.0,
                    }

        # 1. Combine and prepare prices (use log prices for stationarity assumption)
        combined_df = pd.DataFrame({
            asset1: df1['close'].apply(np.log),
            asset2: df2['close'].apply(np.log)
        }).dropna()

        # Ensure there are enough points after combining
        if len(combined_df) < window:
            return {'error': 'Insufficient data after alignment.', 'z_score_series': pd.Series(), 'hedge_ratio': pd.NA}

        # Define Y (dependent) and X (independent/regressor) variables
        Y = combined_df[asset1]
        X = combined_df[asset2]
        X = sm.add_constant(X)  # Add the constant (intercept) for OLS

        # 2. Perform OLS Regression
        try:
            ols_model = sm.OLS(Y, X).fit()
            hedge_ratio = ols_model.params[asset2]  # The beta coefficient

            # 3. Calculate Spread (Residuals)
            # Spread = Y - (alpha + beta * X)
            spread = ols_model.resid

        except Exception as e:
            # Handle cases where OLS fails (e.g., singular matrix)
            return {'error': f'OLS calculation failed: {e}', 'z_score_series': pd.Series(), 'hedge_ratio': pd.NA}

        # 4. Calculate Z-Score (Rolling)
        # Only calculate Z-score over the spread since it's the target series
        rolling_mean = spread.rolling(window=window).mean()
        rolling_std = spread.rolling(window=window).std()

        # Handle division by zero for STD (occurs early in the series)
        z_score = (spread - rolling_mean) / rolling_std.replace(0, pd.NA)

        # 5. Perform Augmented Dickey-Fuller (ADF) Test
        # ADF is only meaningful if run on the spread series
        adf_p_value = pd.NA
        if len(spread) > 10:  # ADF requires a minimum number of observations
            adf_result = adfuller(spread.dropna(), autolag='AIC')
            adf_p_value = adf_result[1]  # The p-value is the second element

        # 6. Assemble Results
        return {
            'asset1': asset1,
            'asset2': asset2,
            'hedge_ratio': hedge_ratio,
            'spread_series': spread.tail(window),  # Only show the relevant window
            'z_score_series': z_score.tail(window),  # Only show the relevant window
            'adf_p_value': adf_p_value,
            'latest_z_score': z_score.iloc[-1] if not z_score.empty and not pd.isna(z_score.iloc[-1]) else 0.0,
            'latest_spread': spread.iloc[-1] if not spread.empty else 0.0,
        }

    def close(self):
        """Closes the DuckDB connection."""
        self.db_conn.close()


# --- Required Integration: The Orchestration Script (app.py) ---
# This is the final piece of the backend puzzle, using Streamlit for the frontend.

### 📝 `app.py` (The main execution and orchestration script)


