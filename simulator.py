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

FUND_NAMES = [item for sublist in data_loader.tailored.values() for item in sublist]

def simulate():
    initial_capital = 100000
    num_funds = len(FUND_NAMES)
    initial_fund_capital = initial_capital / num_funds
    holdings = {}
    
    funds = parsers.load(FUND_NAMES)
    df = pd.DataFrame(funds).dropna()

    start_date = df.index[0]
    end_date = df.index[-1]
    
    daterange = pd.date_range(start_date, end_date, freq='MS')
    values = []
    for date in daterange:
        
        # Initialize
        if date == start_date:
            for fund_name in FUND_NAMES:
                price = df.at[date, fund_name]
                shares = initial_fund_capital / price
                holdings[fund_name] = shares

        value = 0
        for fund_name in FUND_NAMES:
            price = df.at[date, fund_name]
            shares = holdings[fund_name]
            value = value + (shares * price)
        
        values.append(value)

        # Rebalance
        timestamp = pd.Timestamp(date)
        if timestamp.is_year_start:
            fund_capital = value / num_funds
            for fund_name in FUND_NAMES:
                price = df.at[date, fund_name]
                shares = fund_capital / price
                holdings[fund_name] = shares
            logger.info('Rebalanced on %s to: %s', timestamp, holdings)
            
    series = pd.Series(values, daterange).pct_change().dropna()
    logger.info(('skew', stats.skew(series, nan_policy='raise')))
    logger.info(('cagr', emp.cagr(series, emp.MONTHLY)))
    logger.info(('vol', emp.annual_volatility(series, emp.MONTHLY)))
    

simulate()        