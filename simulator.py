import logging, sys
import math
from functools import reduce
from math import sqrt
import pandas as pd
import empyrical as emp
from scipy import stats
import parsers
import data_loader
from skill_metric import skill_metric

logging.basicConfig(stream=sys.stderr, level=logging.DEBUG)
logger = logging.getLogger('simulator')
logger.setLevel(logging.INFO)

def load():
    dfs = {}
    for _, rec in data_loader.tailored.items():
        rec_type = rec[0]
        
        if rec_type == 'single':
            fund_name = rec[1]
            dfs[fund_name] = parsers.load([fund_name])[fund_name]
        else: raise "Unrecognized load record type."
        
    return pd.DataFrame(dfs).dropna()

def compute_stats(value_series):    
    series = value_series.pct_change().dropna()
    af = math.sqrt(12)
    skew = series.skew() / af
    kurt = series.kurt() / 12
    sigma = series.std()
    mu = series.mean()
    sm = skill_metric(mu, sigma, skew)
    if sm:
        tau = sm * af
    else:
        tau = None
    
    d = {
        'cagr': emp.cagr(series, emp.MONTHLY),
        'vol': emp.annual_volatility(series, emp.MONTHLY),
        'sharpe': emp.sharpe_ratio(series, 0, emp.MONTHLY),
        'tau': tau,
        'skew': skew,
        'kurt': kurt
    }
        
    return pd.Series(d)
    
def make_stats_df(data_df, simulation_series):
    d = {}
    for fund_name, fund_series in data_df.items():
        d[fund_name] = compute_stats(fund_series)
        
    d['simulation'] = compute_stats(simulation_series)
    
    return pd.DataFrame(d)

def simulate():
    df = load()

    initial_capital = 100000
    fund_names = df.columns
    num_funds = len(fund_names)
    initial_fund_capital = initial_capital / num_funds
    holdings = {}

    start_date = df.index[0]
    end_date = df.index[-1]
    
    daterange = pd.date_range(start_date, end_date, freq='MS')
    values = []
    for date in daterange:
        
        # Initialize
        if date == start_date:
            for fund_name in fund_names:
                price = df.at[date, fund_name]
                shares = initial_fund_capital / price
                holdings[fund_name] = shares

        value = 0
        for fund_name in fund_names:
            price = df.at[date, fund_name]
            shares = holdings[fund_name]
            value = value + (shares * price)
        
        values.append(value)

        # Rebalance
        timestamp = pd.Timestamp(date)
        if timestamp.is_year_start:
            fund_capital = value / num_funds
            for fund_name in fund_names:
                price = df.at[date, fund_name]
                shares = fund_capital / price
                holdings[fund_name] = shares
            logger.info('Rebalanced on %s to: %s', timestamp, holdings)
            
    simulation_series = pd.Series(values, daterange)    
    
    stats_df = make_stats_df(df, simulation_series)
    print(stats_df)

simulate()        

# TODO: in order to do fallback, need to convert to pct_change, then convert back
# for share pricing to work.

# TODO: create df of stats for full run an all constituents

# Try vol-adjusted rebalancing (only makes sense for multiple funds within a category)