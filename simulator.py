import logging, sys
from functools import reduce
import pandas as pd
import empyrical as emp
from scipy import stats
import parsers
import data_loader

logging.basicConfig(stream=sys.stderr, level=logging.DEBUG)
logger = logging.getLogger('simulator')
logger.setLevel(logging.INFO)

def load():
    dfs = {}
    for _, rec in data_loader.tailored.items():
        rec_type = rec[0]
        
        if rec_type == 'fallback':
            primary, fallback = rec[1:]
            df = pd.DataFrame(parsers.load([primary, fallback])).pct_change()
            # TODO: convert back to prices
            dfs[primary + '*'] = pd.Series(df.loc[:, primary].fillna(df.loc[:, fallback]))
        elif rec_type == 'single':
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
    logger.info(('skew', stats.skew(series, nan_policy='raise')))
    logger.info(('cagr', emp.cagr(series, emp.MONTHLY)))
    logger.info(('vol', emp.annual_volatility(series, emp.MONTHLY)))
    

simulate()        

# TODO: in order to do fallback, need to convert to pct_change, then convert back
# for share pricing to work.