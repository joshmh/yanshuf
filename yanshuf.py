import pandas as pd
import bt
import data_loader
import matplotlib.pyplot as plt
import ffn

from itertools import islice, product, chain
from monthdelta import monthmod
from parsers import load_all
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


data = load_all()


def run_all(keylists):
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


def run_alts():
    groups = data_loader.alt_groups()
    df = run_all(groups)
    df.sort_values('score', inplace=True, ascending=False)
    return df.style\
        .format(to_pct_fmt)\
        .format(to_int_fmt, subset=['months'])\
        .format(to_float_fmt,
                subset=['explosivity', 'correlation', 'score', 'calmar', 'monthly_skew', 'monthly_sharpe'])\
        .render()


def run_long_vol():
    groups = data_loader.long_vol_groups()
    df = run_all(groups)
    df.sort_values('score', inplace=True, ascending=False)
    return df.style\
        .format(to_pct_fmt)\
        .format(to_int_fmt, subset=['months'])\
        .format(to_float_fmt,
                subset=['explosivity', 'correlation', 'score', 'calmar', 'monthly_skew', 'monthly_sharpe'])\
        .render()


def run_commodity_trend():
    groups = data_loader.commodity_trend_groups()
    df = run_all(groups)
    df.sort_values('score', inplace=True, ascending=False)
    return df.style\
        .format(to_pct_fmt)\
        .format(to_int_fmt, subset=['months'])\
        .format(to_float_fmt,
                subset=['correlation', 'score', 'calmar', 'monthly_skew', 'monthly_sharpe'])\
        .render()


def run_dragon():
    weights = data_loader.dragon
    keys = weights.keys()

    filtered_data = {k: v for k, v in data.items() if k in keys}
    df = pd.DataFrame(filtered_data).loc['2005-01':'2019-12']
    df.dropna(inplace=True)

    strategy_name = 'dragon'
    s = bt.Strategy(strategy_name, [bt.algos.RunQuarterly(),
                                    bt.algos.SelectAll(),
                                    bt.algos.WeighSpecified(**weights),
                                    bt.algos.Rebalance()])

    test = bt.Backtest(s, df, progress_bar=False)
    res = bt.run(test)
    ss = res.stats[strategy_name]
    months_rec = monthmod(ss['start'], ss['end'])
    months = months_rec[0].months

    return_table = res[strategy_name].return_table

    extras = pd.DataFrame([pd.Series({
        '2019': return_table.at[2019, 'YTD'],
        '2018': return_table.at[2018, 'YTD'],
        'months': months
    }, name=strategy_name)])

    index = ['months', 'cagr', '2019', '2018', 'monthly_vol', 'max_drawdown',
             'calmar', 'monthly_skew', 'monthly_sharpe',
             'best_month', 'worst_month', 'best_year', 'worst_year'
             ]

    filtered_stats = res.stats.filter(
        stat_keys, axis='index')

    # re-order columns
    final = filtered_stats.transpose().join(extras)[index]

    return final.style\
        .format(to_pct_fmt)\
        .format(to_int_fmt, subset=['months'])\
        .format(to_float_fmt,
                subset=['calmar', 'monthly_skew', 'monthly_sharpe'])\
        .render()


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

    return_table = res[strategy_name].return_table
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

    index = ['score', 'months', 'cagr', 'mar-2020', '2020', '2019', '2018', 'monthly_vol', 'max_drawdown',
             'calmar', 'monthly_skew', 'monthly_sharpe',
             'best_month', 'worst_month', 'best_year', 'worst_year'
             ]

    # re-order columns
    final = filtered_stats.transpose().join(extras)[index]

    return final


def run_all_dragon():
    long_vol_groups = product(data_loader.long_vol_groups(), algo_stacks)
    commodity_trend_groups = product(
        data_loader.commodity_trend_groups(), algo_stacks)
    combos = list(product(long_vol_groups, commodity_trend_groups))
    dragon_weights = {'sp-500': 0.24, 'TLT': 0.18,
                      'long_vol': 0.21, 'commodity_trend': 0.18, 'gold-oz-usd': 0.19}

    dfs = []
    print(len(combos))
    for combo in combos[:10]:
        long_vol_set, commodity_trend_set = combo
        long_vol_group, long_vol_strat = long_vol_set
        commodity_trend_group, commodity_trend_strat = commodity_trend_set
        keys = long_vol_group + commodity_trend_group + \
            ('sp-500', 'TLT', 'gold-oz-usd')
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
                                                                       'sp-500', 'TLT', 'gold-oz-usd'])
        df = dragon_backtest(keys, strategy, data)
        dfs.append(df)

    final = pd.concat(dfs).sort_values(
        'score', ascending=False).iloc[:10]

    return final.style\
        .format(to_pct_fmt)\
        .format(to_int_fmt, subset=['months'])\
        .format(to_float_fmt,
                subset=['calmar', 'monthly_skew', 'monthly_sharpe'])\
        .render()

    # dunn-wma:fund-514@qrv
    # tail-reaper:kohinoor-core@qrv
    # polar-star-snn:blackbird-alpha-2.5@qre
