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
    fund_name = 'drury-di'
    sp500 = 'sp500'
    return pd.DataFrame({
        fund_name: parsers.load([fund_name])[fund_name],
        sp500: parsers.load([sp500])[sp500]
    }).dropna()

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
    
def make_stats_df(data_df):
    d = {}
    for fund_name, fund_series in data_df.items():
        d[fund_name] = compute_stats(fund_series)
            
    return pd.DataFrame(d)

def make_corr_df(data_df):
    return data_df.pct_change().corr()

def test():
    df = load()
    
    stats = make_stats_df(df)
    print(stats)
    print(make_corr_df(df))

test()        

# Very close on all measures except skew/kurtosis for blackbird, some exact. 
# Same for aspect div
# Same for dunn-wma except sp500 correlation is off
# Everything very close for drury div