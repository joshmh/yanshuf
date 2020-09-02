import logging, sys
import math
from functools import reduce
from math import sqrt
import pandas as pd
import empyrical as emp
from scipy import stats
import parsers
import data_loader
import fund_tree
from skill_metric import skill_metric

logging.basicConfig(stream=sys.stderr, level=logging.DEBUG)
logger = logging.getLogger('simulator')
logger.setLevel(logging.INFO)

fund_data = fund_tree.flatten(data_loader.simulation)
initial_capital = 1_000_000

def load():
    dfs = {}
    for fund_name in fund_data.keys():
        dfs[fund_name] = parsers.load([fund_name])[fund_name]
        
    return pd.DataFrame(dfs).dropna()

def compute_stats(value_series):    
    series = value_series.pct_change().dropna()
    af = math.sqrt(12)
    raw_skew = series.skew()
    raw_kurt = series.kurt()
    skew = raw_skew / af
    kurt = raw_kurt / 12
    sigma = series.std() * af
    mu = series.mean() * 12
    tau = skill_metric(mu, sigma, skew)
    
    d = {
        'calmar': emp.calmar_ratio(series, emp.MONTHLY),
        'tau': tau,
        'cagr': emp.cagr(series, emp.MONTHLY),
        'max_drawdown': emp.max_drawdown(series),
        'vol': sigma,
        'sharpe': mu / sigma,
        'skew': skew,
        'kurt': kurt,
        'raw_skew': raw_skew,
        'raw_kurt': raw_kurt
    }
        
    return pd.Series(d)
    
def make_stats_df(data_df, simulation_series):
    d = {}
    for fund_name, fund_series in data_df.items():
        d[fund_name] = compute_stats(fund_series)
        
    d['simulation'] = compute_stats(simulation_series)
    
    return pd.DataFrame(d)

def make_corr_df(data_df):
    return data_df.pct_change().corr()

def is_rebalance(timestamp):
    return timestamp.is_year_start

def fund_amounts():
    d = {}
    for name, pct in fund_data.items():
        d[name] = round(pct * initial_capital)
    return d

def simulate():
    df = load()

    holdings = {}

    start_date = df.index[0]
    end_date = df.index[-1]
    
    daterange = pd.date_range(start_date, end_date, freq='MS')
    values = []
    for date in daterange:
        
        # Initialize
        if date == start_date:
            for fund_name, pct in fund_data.items():
                price = df.at[date, fund_name]
                shares = (initial_capital * pct) / price
                holdings[fund_name] = shares

        current_fund_value = 0
        for fund_name in fund_data.keys():
            price = df.at[date, fund_name]
            shares = holdings[fund_name]
            current_fund_value += (shares * price)
        
        values.append(current_fund_value)

        # Rebalance
        timestamp = pd.Timestamp(date)
        if is_rebalance(timestamp):
            for fund_name, pct in fund_data.items():
                price = df.at[date, fund_name]
                shares = (current_fund_value * pct) / price
                holdings[fund_name] = shares
            logger.info('Rebalanced on %s to: %s', timestamp, holdings)
            
    simulation_series = pd.Series(values, daterange)    
    
    stats_df = make_stats_df(df, simulation_series)
    print(stats_df)
    print(make_corr_df(df))
    print(f'Based on {df.shape[1]} funds over {len(daterange)} months.')
    print(fund_amounts())
    print(start_date)
    print(end_date)

simulate()        

# TODO: in order to do fallback, need to convert to pct_change, then convert back
# for share pricing to work.

# TODO: create df of stats for full run an all constituents

# Try vol-adjusted rebalancing (only makes sense for multiple funds within a category)