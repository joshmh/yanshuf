import logging, sys
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
            
    series = pd.Series(values, daterange).pct_change().dropna()
    skew = series.skew()
    sigma = series.std()
    mu = series.mean()
    tau = skill_metric(mu, sigma, skew) * sqrt(12)
    
    logger.info(('skew', skew))
    logger.info(('cagr', emp.cagr(series, emp.MONTHLY)))
    logger.info(('vol', emp.annual_volatility(series, emp.MONTHLY)))
    logger.info(('sigma', sigma))
    logger.info(('sharpe', emp.sharpe_ratio(series, 0, emp.MONTHLY)))
    logger.info(('tau', tau))
    

simulate()        

# TODO: in order to do fallback, need to convert to pct_change, then convert back
# for share pricing to work.