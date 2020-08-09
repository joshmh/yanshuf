import logging, sys
logging.basicConfig(stream=sys.stderr, level=logging.INFO)

import pandas as pd
import bt
import data_loader
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick

from data_loader import tailored
from itertools import product, chain, combinations
from monthdelta import monthmod
from parsers import load
from math import isnan

algo_stacks = [
    ('qrv', [bt.algos.RunQuarterly(),
             bt.algos.SelectAll(),
             bt.algos.WeighInvVol(),
             bt.algos.Rebalance()]
     ),
    ('qre', [bt.algos.RunQuarterly(),
             bt.algos.SelectAll(),
             bt.algos.WeighEqually(),
             bt.algos.Rebalance()]
     )
]

stat_keys = ['max_drawdown', 'monthly_vol', 'best_month', 'best_year', 'worst_month',
             'worst_year', 'monthly_skew', 'monthly_sharpe', 'cagr', 'calmar', 'three_month']


def compile_data(minimum_months, keys):
    data = load(keys)
    res = {k: v for k, v in data.items(
    ) if k in keys and v.size > minimum_months}

    return res

def to_float_fmt(f):
    return "None" if isnan(f) else f"{f:.3f}"


def to_pct_fmt(f):
    return "None" if isnan(f) else f"{f:.2%}"


def to_int_fmt(f):
    return f'{f:.0f}'

def dragon_backtest(keys, strategy, data):
    filtered_data = {k: v for k, v in data.items() if k in keys}
    df = pd.DataFrame(filtered_data)
    df.dropna(inplace=True)
    
    test = bt.Backtest(strategy, df, progress_bar=False)
    res = bt.run(test)

    filtered_stats = res.stats.filter(stat_keys, axis='index')
    strategy_name = strategy.name
    ss = res.stats[strategy_name]

    logging.info(res)    
    logging.info('end: %s', ss['end'])
    months_rec = monthmod(ss['start'], ss['end'])
    months = months_rec[0].months
    ps = res[strategy_name]
    return_table = ps.return_table
    mar_2020 = return_table.at[2020, 'Mar']
    ytd_2020 = return_table.at[2020, 'YTD']
    score = ss['cagr']
    extras = pd.DataFrame([pd.Series({
        'mar-2020': mar_2020,
        '2020': ytd_2020,
        '2019': return_table.at[2019, 'YTD'],
        '2018': return_table.at[2018, 'YTD'],
        'months': months,
        'score': score
    }, name=strategy_name)])

    index = ['months', 'cagr', 'mar-2020', '2020', '2019', '2018', 'three_month', 'monthly_vol', 'max_drawdown',
             'calmar', 'monthly_skew', 'monthly_sharpe',
             'best_month', 'worst_month', 'best_year', 'worst_year'
             ]

    # re-order columns
    final = filtered_stats.transpose().join(extras)[index]

    return final, df


def group_info(group):
    logging.info(group)
    return [(key, data_loader.shortnames[key]) for key in group]

def tailored_info():
    return [(group_name, group_info(group)) for (group_name, group) in tailored.items() if len(group) > 0]

def run_tailored_dragon():
    stock_ticker = tailored['stocks']
    gold_ticker = tailored['gold']
    qrv = algo_stacks[0]
    qre = algo_stacks[1]

    long_vol_group = tailored['long_vol_group']
    long_vol_strat = qrv
    
    commodity_trend_group = tailored['commodity_trend_group']
    commodity_trend_strat = qrv

    alt_group = tailored['alt_group']
    alt_strat = qrv

    bond_group = tailored['bonds']
    bond_strat = qre

    gold_group = tailored['gold']
    gold_strat = qre

    stocks_group = tailored['stocks']
    stocks_strat = qre
    
    keys = long_vol_group + commodity_trend_group + \
        alt_group + bond_group + stocks_group + gold_group
    
    logging.info(keys)
    
    data = compile_data(55, keys)

    strategy_name = 'tailored-dragon'
    long_vol_strategy = bt.Strategy(
        'long_vol', long_vol_strat[1], list(long_vol_group))
    commodity_trend_strategy = bt.Strategy(
        'commodity_trend', commodity_trend_strat[1], list(commodity_trend_group))
    alt_strategy = bt.Strategy(
        'alt', alt_strat[1], list(alt_group))
    bond_strategy = bt.Strategy(
        'bonds', bond_strat[1], list(bond_group))
    gold_strategy = bt.Strategy(
        'gold', gold_strat[1], list(gold_group))
    stocks_strategy = bt.Strategy(
        'stocks', stocks_strat[1], list(stocks_group))
    
    strategy = bt.Strategy(strategy_name, [bt.algos.RunQuarterly(),
                                           bt.algos.SelectAll(),
                                           bt.algos.WeighEqually(),
                                           bt.algos.Rebalance()], [long_vol_strategy,
                                                                   commodity_trend_strategy, alt_strategy,
                                                                   bond_strategy, stocks_strategy, gold_strategy])
    stats, sources = dragon_backtest(keys, strategy, data)
    
    performance_table = stats.style.format(to_pct_fmt)\
        .format(to_int_fmt, subset=['months'])\
        .format(to_float_fmt,
                subset=['calmar', 'monthly_skew', 'monthly_sharpe'])\
        .render()
    
    logging.info(sources)
    
    corr_table = sources.rename(columns=data_loader.shortnames).pct_change().corr()\
        .style.background_gradient(cmap='coolwarm')\
        .set_precision(2).render()

    info = tailored_info()
        
    return performance_table, corr_table, info
