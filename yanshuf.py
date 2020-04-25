import pandas as pd
import bt
import data_loader
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick
import ffn

from datetime import date
from itertools import islice, product, chain, combinations
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
             'worst_year', 'monthly_skew', 'monthly_sharpe', 'cagr', 'calmar']


def compile_data(minimum_months, keys):
    data = load()
    res = {k: v for k, v in data.items(
    ) if k in keys and v.size > minimum_months}

    return res


def nm(val, base):
    return min([base, val]) / base


def backtest(algo_stack, keys, data):
    filtered_data = {k: v for k, v in data.items() if k in keys}
    df = pd.DataFrame(filtered_data)
    df.dropna(inplace=True)
    strategy_name = ':'.join(keys) + '@' + algo_stack[0]
    s = bt.Strategy(strategy_name, algo_stack[1])

    test = bt.Backtest(s, df, progress_bar=False)
    res = bt.run(test)

    dfp = df.pct_change()
    corr = dfp.corr().iat[0, 1] if dfp.shape[1] == 2 else None
    filtered_stats = res.stats.filter(
        stat_keys, axis='index')

    ss = res.stats[strategy_name]
    months_rec = monthmod(ss['start'], ss['end'])
    months = months_rec[0].months

    return_table = res[strategy_name].return_table
    mar_2020 = return_table.at[2020, 'Mar']
    ytd_2020 = return_table.at[2020, 'YTD']
    explosivity = mar_2020 / (2*-ss['max_drawdown'])
    score = ss['calmar']
    extras = pd.DataFrame([pd.Series({
        'correlation': corr,
        'explosivity': explosivity,
        'mar-2020': mar_2020,
        '2020': ytd_2020,
        '2019': return_table.at[2019, 'YTD'],
        '2018': return_table.at[2018, 'YTD'],
        'months': months,
        'score': score
    }, name=strategy_name)])

    index = ['score', 'months', 'correlation', 'cagr', 'explosivity', 'mar-2020', '2020', '2019', '2018', 'monthly_vol', 'max_drawdown',
             'calmar', 'monthly_skew', 'monthly_sharpe',
             'best_month', 'worst_month', 'best_year', 'worst_year'
             ]

    # re-order columns
    final = filtered_stats.transpose().join(extras)[index]

    return final


def run_all(keylists, data):
    dfs = []
    for keylist in keylists:
        for algo_stack in algo_stacks:
            df_new = backtest(algo_stack, keylist, data)
            dfs.append(df_new)
    return pd.concat(dfs)


def long_vol_score(df):
    dd = df[0]
    return dd


def to_float_fmt(f):
    return "None" if isnan(f) else f"{f:.3f}"


def to_pct_fmt(f):
    return "None" if isnan(f) else f"{f:.2%}"


def to_int_fmt(f):
    return f'{f:.0f}'


def boxplot(name, df):
    fig, axs = plt.subplots(figsize=(12, 4))
    axs.yaxis.set_major_formatter(mtick.PercentFormatter(1.0))
    df.loc[:, ['cagr', '2020', '2018', 'worst_year',
               'worst_month', 'max_drawdown']].plot.box(ax=axs, grid=True)
    fig.savefig(f"plots/{name}-boxplot.png")


def run_alts():
    all_keys = data_loader.asset_classes['alts']
    data = compile_data(60, all_keys)
    groups = combinations(data.keys(), 2)
    df = run_all(groups, data)
    df.sort_values('score', inplace=True, ascending=False)
    boxplot('alts', df)
    return df.style\
        .format(to_pct_fmt)\
        .format(to_int_fmt, subset=['months'])\
        .format(to_float_fmt,
                subset=['explosivity', 'correlation', 'score', 'calmar', 'monthly_skew', 'monthly_sharpe'])\
        .render()


def run_long_vol():
    all_keys = data_loader.asset_classes['long_vol']
    data = compile_data(60, all_keys)
    groups = combinations(data.keys(), 2)

    df = run_all(groups, data)
    df.sort_values('score', inplace=True, ascending=False)
    return df.style\
        .format(to_pct_fmt)\
        .format(to_int_fmt, subset=['months'])\
        .format(to_float_fmt,
                subset=['explosivity', 'correlation', 'score', 'calmar', 'monthly_skew', 'monthly_sharpe'])\
        .render()


def run_commodity_trend():
    all_keys = data_loader.asset_classes['commodity_trend']
    data = compile_data(60, all_keys)
    groups = combinations(data.keys(), 2)

    df = run_all(groups, data)
    df.sort_values('score', inplace=True, ascending=False)
    return df.style\
        .format(to_pct_fmt)\
        .format(to_int_fmt, subset=['months'])\
        .format(to_float_fmt,
                subset=['correlation', 'score', 'calmar', 'monthly_skew', 'monthly_sharpe'])\
        .render()


# def run_dragon():
#     weights = data_loader.dragon
#     keys = weights.keys()

#     filtered_data = {k: v for k, v in data.items() if k in keys}
#     df = pd.DataFrame(filtered_data).loc['2005-01':'2019-12']
#     df.dropna(inplace=True)

#     strategy_name = 'dragon'
#     s = bt.Strategy(strategy_name, [bt.algos.RunQuarterly(),
#                                     bt.algos.SelectAll(),
#                                     bt.algos.WeighSpecified(**weights),
#                                     bt.algos.Rebalance()])

#     test = bt.Backtest(s, df, progress_bar=False)
#     res = bt.run(test)
#     ss = res.stats[strategy_name]
#     months_rec = monthmod(ss['start'], ss['end'])
#     months = months_rec[0].months

#     return_table = res[strategy_name].return_table

#     extras = pd.DataFrame([pd.Series({
#         '2019': return_table.at[2019, 'YTD'],
#         '2018': return_table.at[2018, 'YTD'],
#         'months': months
#     }, name=strategy_name)])

#     index = ['months', 'cagr', '2019', '2018', 'monthly_vol', 'max_drawdown',
#              'calmar', 'monthly_skew', 'monthly_sharpe',
#              'best_month', 'worst_month', 'best_year', 'worst_year'
#              ]

#     filtered_stats = res.stats.filter(
#         stat_keys, axis='index')

#     # re-order columns
#     final = filtered_stats.transpose().join(extras)[index]

#     return final.style\
#         .format(to_pct_fmt)\
#         .format(to_int_fmt, subset=['months'])\
#         .format(to_float_fmt,
#                 subset=['calmar', 'monthly_skew', 'monthly_sharpe'])\
#         .render()


def dragon_backtest(keys, strategy, data):
    filtered_data = {k: v for k, v in data.items() if k in keys}
    df = pd.DataFrame(filtered_data)
    df.dropna(inplace=True)

    test = bt.Backtest(strategy, df, progress_bar=False)
    res = bt.run(test)

    filtered_stats = res.stats.filter(stat_keys, axis='index')
    strategy_name = strategy.name
    ss = res.stats[strategy_name]
    months_rec = monthmod(ss['start'], ss['end'])
    months = months_rec[0].months
    ps = res[strategy_name]
    return_table = ps.return_table
    mar_2020 = return_table.at[2020, 'Mar']
    ytd_2020 = return_table.at[2020, 'YTD']
    # mar_2020 = None
    # ytd_2020 = None
    score = ss['cagr']
    extras = pd.DataFrame([pd.Series({
        'mar-2020': mar_2020,
        '2020': ytd_2020,
        '2019': return_table.at[2019, 'YTD'],
        '2018': return_table.at[2018, 'YTD'],
        'months': months,
        'score': score
    }, name=strategy_name)])

    index = ['score', 'months', 'cagr', 'mar-2020', '2020', '2019', '2018', 'monthly_vol', 'max_drawdown',
             'calmar', 'monthly_skew', 'monthly_sharpe',
             'best_month', 'worst_month', 'best_year', 'worst_year'
             ]

    # re-order columns
    final = filtered_stats.transpose().join(extras)[index]

    return final


def run_all_dragon():
    stock_ticker = 'ACWI'
    long_vol = data_loader.asset_classes['long_vol']
    commodity_trend = data_loader.asset_classes['commodity_trend']
    other = (stock_ticker, 'TLT', 'gold-oz-usd')
    all_keys = list(chain(long_vol, commodity_trend, other))
    data = compile_data(55, all_keys)
    keys = data.keys()
    long_vol_funds = set(long_vol).intersection(keys)
    commodity_trend_funds = set(commodity_trend).intersection(keys)
    long_vol_groups = product(combinations(long_vol_funds, 2), algo_stacks)
    commodity_trend_groups = product(
        combinations(commodity_trend_funds, 2), algo_stacks)
    combos = list(product(long_vol_groups, commodity_trend_groups))
    dragon_weights = {stock_ticker: 0.24, 'TLT': 0.18,
                      'long_vol': 0.21, 'commodity_trend': 0.18, 'gold-oz-usd': 0.19}

    dfs = []
    for combo in combos:
        long_vol_set, commodity_trend_set = combo
        long_vol_group, long_vol_strat = long_vol_set
        commodity_trend_group, commodity_trend_strat = commodity_trend_set
        keys = long_vol_group + commodity_trend_group + other
        strategy_name = f"{':'.join(long_vol_group)}@{long_vol_strat[0]};{':'.join(commodity_trend_group)}@{commodity_trend_strat[0]}"
        long_vol_strategy = bt.Strategy(
            'long_vol', long_vol_strat[1], list(long_vol_group))
        commodity_trend_strategy = bt.Strategy(
            'commodity_trend', commodity_trend_strat[1], list(commodity_trend_group))
        strategy = bt.Strategy(strategy_name, [bt.algos.RunQuarterly(),
                                               bt.algos.SelectAll(),
                                               bt.algos.WeighSpecified(
            **dragon_weights),
            bt.algos.Rebalance()], [long_vol_strategy,
                                    commodity_trend_strategy,
                                    stock_ticker, 'TLT', 'gold-oz-usd'])
        df = dragon_backtest(keys, strategy, data)
        dfs.append(df)

    pds = pd.concat(dfs).sort_values(
        'score', ascending=False)

    boxplot('dragon-orig', pds)
    final = pds.iloc[:100]

    return final.style.format(to_pct_fmt)\
        .format(to_int_fmt, subset=['months'])\
        .format(to_float_fmt,
                subset=['calmar', 'monthly_skew', 'monthly_sharpe'])\
        .render()


def run_tailored_dragon():
    stock_ticker = 'ACWI'
    qrv = algo_stacks[0]
    qre = algo_stacks[1]

    dragon_weights = {stock_ticker: 0.12, 'TLT': 0.09,
                      'long_vol': 0.21, 'commodity_trend': 0.18, 'gold-oz-usd': 0.19, 'alt': 0.21}

    tailored = data_loader.tailored
    long_vol_group = tailored['long_vol_group']
    long_vol_strat = qrv
    commodity_trend_group = tailored['commodity_trend_group']
    commodity_trend_strat = qrv
    alt_group = tailored['alt_group']
    alt_strat = qre

    keys = long_vol_group + commodity_trend_group + \
        alt_group + [stock_ticker, 'TLT', 'gold-oz-usd']
    data = compile_data(55, keys)

    dfs = []
    strategy_name = 'tailored-dragon'
    long_vol_strategy = bt.Strategy(
        'long_vol', long_vol_strat[1], list(long_vol_group))
    commodity_trend_strategy = bt.Strategy(
        'commodity_trend', commodity_trend_strat[1], list(commodity_trend_group))
    alt_strategy = bt.Strategy(
        'alt', alt_strat[1], list(alt_group))
    strategy = bt.Strategy(strategy_name, [bt.algos.RunQuarterly(),
                                           bt.algos.SelectAll(),
                                           bt.algos.WeighEqually(),
                                           bt.algos.Rebalance()], [long_vol_strategy,
                                                                   commodity_trend_strategy, alt_strategy,
                                                                   stock_ticker, 'TLT', 'gold-oz-usd'])
    df = dragon_backtest(keys, strategy, data)

    return df.style.format(to_pct_fmt)\
        .format(to_int_fmt, subset=['months'])\
        .format(to_float_fmt,
                subset=['calmar', 'monthly_skew', 'monthly_sharpe'])\
        .render()
