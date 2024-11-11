# %%
import sys, os
sys.path.append('s:/Codebase/Data_Importer')
from bg_data_importer_test import DataImporter
import pandas as pd
import talib
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import streamlit as st
st.set_page_config(layout="wide")
# %%
def get_price_data():
    price_path = rf"S:\Codebase\Datahub\Adhoc\ray_prices_{datetime.today().strftime('%Y%m%d')}.prq"
    ticker_path = rf"S:\Codebase\Datahub\Adhoc\ray_tickers_{datetime.today().strftime('%Y%m%d')}.prq"
    if os.path.exists(price_path) and os.path.exists(ticker_path):
        prices = pd.read_parquet(price_path)
        seclist = pd.read_parquet(ticker_path)
    else:
        rdate = '2024-10-31'
        with DataImporter() as data:
            query = f"""
            
            select distinct fsym_id, ticker_orig, proper_name 
            from Development.dbo.BMC_Monthly 
            where bm_id in ('sp500','r2500') and rdate = '{rdate}'
            
            """
            seclist = data.load_data(query)
            seclist['ticker_orig'] = seclist['ticker_orig'].str.replace(" US", "")
            seclist.to_parquet(ticker_path)
            
            query = f"""
            
            
            with seclist as (
                select distinct fsym_id, ticker_orig, proper_name 
                from Development.dbo.BMC_Monthly 
                where bm_id in ('sp500','r2500') and rdate = '{rdate}'
        )

            select ap.*, --ap.fsym_id, ap.rdate, adj_price, 
            cum_div_factor*cum_spec_factor*cum_split_factor as cumulative_factors , 
            p_price_high as price_high, p_price_low as price_low, p_volume as volume
            from seclist
            join fstest.dbo.AdjustedPrice ap on ap.fsym_id = seclist.fsym_id
            join fstest.fp_v2.fp_basic_prices bp on bp.fsym_id = seclist.fsym_id and bp.p_date = ap.rdate
            where bp.p_date > '2023-12-31' and ap.rdate > '2023-12-31'
            
            """

            prices = data.load_data(query).rename(columns = {'p_date':'rdate'})
            prices.to_parquet(price_path)
        
    # adjust for splits and dividends
    prices['adj_high'] = prices['price_high']*prices['cumulative_factors']
    prices['adj_low'] = prices['price_low']*prices['cumulative_factors']
    prices['adj_volume'] = prices['volume']*prices['cum_split_factor']
    return prices, seclist

# %%
def plotting_indicator(test, indicator_cols, title,  price_col='adj_price',):
    fig = px.line(test, x='rdate', y=indicator_cols, color='variable', title=title, height=600)
    fig.add_scatter(x=test['rdate'], y=test['adj_price'], mode='lines', name='Close', line=dict(width=3, color='purple'), yaxis='y2')
    fig.update_layout(
        yaxis2=dict(title="Price", overlaying="y", side="right"),
        yaxis=dict(title="Indicator Value"),
        legend_title="Variable",
        hovermode='x',
        title_x=0.5
    )
    fig.update_traces(hovertemplate='%{y:.2f}<extra>%{fullData.name}</extra>',)
    
    if 'ADX' in title.upper():
        fig.add_hline(y=25, line_dash="dot", line_color="black", annotation_text="Trend Strength", annotation_position="bottom right")
    if 'VRSI' in title.upper():
        fig.add_hline(y=80, line=dict(color='red', dash='dash'), annotation_text="Overbought (80)")
        fig.add_hline(y=20, line=dict(color='green', dash='dash'), annotation_text="Oversold (20)")
    
    return fig

# %%
# Use MACD for trend changes, ADX to confirm trend strength, and RSI for entry/exit points based on overbought/oversold signals or divergences.

st_offset = st.slider('Short Term Offset', min_value=1, max_value=15, value=5)
lt_offset = st.slider('Long Term Offset', min_value=15, max_value=30, value=15)
signal_offset = st.slider('Signal Offset', min_value=5, max_value=15, value=9)

prices, seclist = get_price_data()
ticker = st.selectbox('Select Ticker', seclist['ticker_orig'])
secid = seclist.loc[seclist['ticker_orig']=='HOFT','fsym_id'].iloc[0]
test = prices[prices['fsym_id']==secid].copy()
# %%
#MACD
# Trending Markets: Shorter periods help capture strong directional moves, ideal for momentum-driven markets. Ideal for momentum-driven markets.
# High-Volatility Assets: Shorter MACD periods may be used to capture frequent shifts in momentum, making it more reactive to quick changes (e.g., cryptocurrency markets).
# Longer periods help avoid frequent whipsaws and false signals, filtering out noise in choppy or sideways markets.

test['MACD'], test['MACD_signal'], test['MACD_hist'] = talib.MACD(test['adj_price'], 
                                                                  fastperiod=st_offset,
                                                                  slowperiod=lt_offset,
                                                                  signalperiod=signal_offset)
fig = plotting_indicator(test, ['MACD', 'MACD_signal', 'MACD_hist'], 'MACD Indicator')
st.plotly_chart(fig)

# %%
# ADX
# ADX > 25 is ideal for trend-following strategies, as it confirms a strong trend.
# +DI above -DI and ADX > 25: Strong uptrend.
# -DI above +DI and ADX > 25: Strong downtrend

test['ADX'] = talib.ADX(test['adj_high'], test['adj_low'], test['adj_price'], timeperiod=st_offset)
test['+DI'] = talib.PLUS_DI(test['adj_high'], test['adj_low'], test['adj_price'], timeperiod=st_offset)
test['-DI'] = talib.MINUS_DI(test['adj_high'], test['adj_low'], test['adj_price'], timeperiod=st_offset)
fig = plotting_indicator(test, ['ADX', '+DI', '-DI'], 'ADX Indicator')
st.plotly_chart(fig)
# %%
# MFI uses both price and volume to determine overbought or oversold conditions
test['MFI'] = talib.MFI(test['adj_high'], test['adj_low'], test['adj_price'], test['adj_volume'], timeperiod=st_offset)
fig = plotting_indicator(test, ['MFI'], 'Money Flow Index (VRSI)')
st.plotly_chart(fig)

