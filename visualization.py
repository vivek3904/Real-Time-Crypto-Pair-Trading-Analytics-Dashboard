import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
from typing import Dict

# --- Configuration for Plotting ---
COLOR_SPREAD = 'darkblue'
COLOR_ZSCORE = 'darkred'


def plot_price_chart(df: pd.DataFrame, symbol: str, timeframe: str) -> go.Figure:
    """Creates a Plotly Candlestick chart for OHLCV data."""
    if df.empty:
        fig = go.Figure()
        fig.add_annotation(text="No data available for price chart.",
                           xref="paper", yref="paper", x=0.5, y=0.5)
        return fig

    fig = make_subplots(rows=2, cols=1,
                        shared_xaxes=True,
                        vertical_spacing=0.08,
                        row_heights=[0.7, 0.3])

    # Candlestick Trace
    fig.add_trace(go.Candlestick(x=df.index,
                                 open=df['open'],
                                 high=df['high'],
                                 low=df['low'],
                                 close=df['close'],
                                 name='Price'), row=1, col=1)

    # Volume Trace
    fig.add_trace(go.Bar(x=df.index,
                         y=df['volume'],
                         name='Volume',
                         marker_color='lightblue'), row=2, col=1)

    fig.update_layout(
        title=f"Price and Volume: {symbol.upper()} ({timeframe})",
        xaxis_title="Time",
        xaxis_rangeslider_visible=False,  # Hide the range slider for a cleaner look
        height=600
    )
    # Ensure charts support zoom, pan, and hover
    fig.update_xaxes(showgrid=True, row=2, col=1)
    fig.update_yaxes(title_text="Price", row=1, col=1)
    fig.update_yaxes(title_text="Volume", row=2, col=1)

    return fig


def plot_spread_and_zscore(metrics: Dict) -> go.Figure:
    """Creates a Plotly figure showing the Spread and Z-Score on two subplots."""
    spread = metrics['spread_series']
    z_score = metrics['z_score_series']

    if spread.empty or z_score.empty:
        fig = go.Figure()
        fig.add_annotation(text="Insufficient data for Spread/Z-Score plot.",
                           xref="paper", yref="paper", x=0.5, y=0.5)
        return fig

    fig = make_subplots(rows=2, cols=1, shared_xaxes=True, vertical_spacing=0.1)

    # 1. Spread Plot (Top)
    fig.add_trace(go.Scatter(x=spread.index, y=spread, mode='lines',
                             name='Spread', line=dict(color=COLOR_SPREAD)), row=1, col=1)

    # Add Mean line (if needed, typically centered around zero)
    fig.add_hline(y=0, line_dash="dash", line_color="gray", row=1, col=1)

    # 2. Z-Score Plot (Bottom)
    fig.add_trace(go.Scatter(x=z_score.index, y=z_score, mode='lines',
                             name='Z-Score', line=dict(color=COLOR_ZSCORE)), row=2, col=1)

    # Add Z-Score Alert Levels (+2, -2) [cite: 8]
    for level in [-2, 2]:
        fig.add_hline(y=level, line_dash="dot", line_color="red", line_width=1,
                      annotation_text=f"{level} Sigma", annotation_position="top left", row=2, col=1)
    fig.add_hline(y=0, line_dash="dash", line_color="gray", row=2, col=1)

    fig.update_layout(title="Mean-Reversion Spread and Z-Score", height=600, showlegend=False)
    fig.update_yaxes(title_text="Spread Value", row=1, col=1)
    fig.update_yaxes(title_text="Z-Score", row=2, col=1)

    return fig


def plot_correlation(correlation_series: pd.Series) -> go.Figure:
    """Creates a Plotly Line chart for rolling correlation."""
    if correlation_series.empty:
        fig = go.Figure()
        fig.add_annotation(text="Insufficient data for Correlation plot.",
                           xref="paper", yref="paper", x=0.5, y=0.5)
        return fig

    fig = go.Figure(data=[
        go.Scatter(x=correlation_series.index, y=correlation_series, mode='lines',
                   name='Correlation', line=dict(color='green'))
    ])

    fig.update_layout(
        title="Rolling Log-Return Correlation",
        yaxis_title="Correlation Value",
        height=300
    )
    # Enforce y-axis boundaries to standard correlation limits
    fig.update_yaxes(range=[-1.05, 1.05], fixedrange=False)

    return fig