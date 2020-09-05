import logging, sys
import math
from functools import reduce
from math import sqrt
import pandas as pd
import empyrical as emp
from scipy import stats
import parsers
import data_loader
import taxes
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
    
def short_name(fund_name):
    if fund_name in data_loader.shortnames:
        return data_loader.shortnames[fund_name]
    else:
        return fund_name
    
def make_stats_df(data_df, simulation_series):
    d = {}
    for fund_name, fund_series in data_df.items():
        d[fund_name] = compute_stats(fund_series)
        
    d['sim'] = compute_stats(simulation_series)
    
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

def sell_purchases(purchases, fund_name, delta_shares):
    cur_purchases = purchases[fund_name]
    basis = 0
    shares_to_sell = delta_shares
    
    while(True):
        (shares, price) = cur_purchases.pop(0)
        logger.info((fund_name, shares_to_sell, shares, len(cur_purchases)))
        if shares_to_sell > shares:
            basis += price * shares
            shares_to_sell -= shares
        else:
            basis += price * shares_to_sell
            remaining = shares - shares_to_sell
            if remaining > 0:
                cur_purchases.insert(0, (remaining, price))
            break
    
    print(purchases[fund_name])
    return basis
        
def simulate():
    df = load()

    holdings = {}
    taxable_cap_gains = 0
    tax_owed = 0
    start_date = df.index[0]
    end_date = df.index[-1]
    
    daterange = pd.date_range(start_date, end_date, freq='MS')
    values = []
    purchases = {}
    for date in daterange:
        
        # Initialize
        if date == start_date:
            for fund_name, pct in fund_data.items():
                price = df.at[date, fund_name]
                shares = (initial_capital * pct) / price
                if taxes.is_taxable(fund_name):
                    purchases[fund_name] = [(shares, price)]
                holdings[fund_name] = shares        
        
        current_fund_value = 0
        for fund_name in fund_data.keys():
            price = df.at[date, fund_name]
            shares = holdings[fund_name]
            current_fund_value += (shares * price)
        
        timestamp = pd.Timestamp(date)

        # Rebalance
        if is_rebalance(timestamp):            
            if tax_owed > 0:
                logger.info(f'Adjusting fund value for {tax_owed} taxes.')
            current_fund_value -= tax_owed
            tax_owed = 0
            
            for fund_name, pct in fund_data.items():
                price = df.at[date, fund_name]
                shares = (current_fund_value * pct) / price
                old_holdings = holdings[fund_name]
                delta_shares = shares - old_holdings

                if taxes.is_taxable(fund_name):
                    if delta_shares < 0:
                        # Sell
                        sell_shares = -delta_shares
                        revenue = sell_shares * price
                        basis = sell_purchases(purchases, fund_name, sell_shares)
                        taxable_cap_gains += revenue - basis
                        logger.info(f'revenue: {revenue}, basis: {basis}, cap_gains: {taxable_cap_gains}')
                    elif delta_shares > 0:
                        # Buy
                        purchases[fund_name].append((delta_shares, price))
                    
                holdings[fund_name] = shares
            logger.info('Rebalanced on %s to: %s', timestamp, holdings)

        # Simple carry-forward of loss
        if timestamp.is_year_start:
            if taxable_cap_gains > 0:
                tax = taxable_cap_gains * taxes.TAX_RATE
                tax_owed += tax
                logger.info(f'tax: {tax}')
                taxable_cap_gains = 0
                
        values.append(current_fund_value)
            
        # TODO: add in capital gains taxation
        # Record purchase batches with prices
        # Handle FIFO buys and sells
        # Deduct taxes when reblancing
            
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