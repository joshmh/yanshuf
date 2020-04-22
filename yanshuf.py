import pandas as pd
import bt
import data_loader
import matplotlib.pyplot as plt
import ffn

from itertools import islice
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

    # fig, axs = plt.subplots(figsize=(12, 12))
    # df.plot.scatter(0, 1, ax=axs)
    # fig.savefig(f"plots/{strategy_name}-scatter.png")
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
    return df


def run_long_vol():
    groups = data_loader.long_vol_groups()
    df = run_all(groups)
    df.sort_values('score', inplace=True, ascending=False)
    return df


def run_commodity_trend():
    groups = data_loader.commodity_trend_groups()
    df = run_all(groups)
    df.sort_values('score', inplace=True, ascending=False)
    return df


def run_dragon():
    weights = data_loader.dragon
    keys = weights.keys()

    filtered_data = {k: v for k, v in data.items() if k in keys}
    df = pd.DataFrame(filtered_data)
    df.dropna(inplace=True)
    strategy_name = 'dragon'
    s = bt.Strategy(strategy_name, [bt.algos.RunQuarterly(),
                                    bt.algos.SelectAll(),
                                    bt.algos.WeighEqually(),
                                    bt.algos.Rebalance()])

    test = bt.Backtest(s, df, progress_bar=False)
    res = bt.run(test)
    ss = res.stats[strategy_name]
    months_rec = monthmod(ss['start'], ss['end'])
    months = months_rec[0].months

    return_table = res[strategy_name].return_table
    mar_2020 = return_table.at[2020, 'Mar']
    ytd_2020 = return_table.at[2020, 'YTD']

    extras = pd.DataFrame([pd.Series({
        'mar-2020': mar_2020,
        '2020': ytd_2020,
        '2019': return_table.at[2019, 'YTD'],
        '2018': return_table.at[2018, 'YTD'],
        'months': months
    }, name=strategy_name)])

    index = ['months', 'cagr', 'mar-2020', '2020', '2019', '2018', 'monthly_vol', 'max_drawdown',
             'calmar', 'monthly_skew', 'monthly_sharpe',
             'best_month', 'worst_month', 'best_year', 'worst_year'
             ]

    filtered_stats = res.stats.filter(
        stat_keys, axis='index')

    # re-order columns
    final = filtered_stats.transpose().join(extras)[index]

    return final


def to_html(df):
    html = df.style\
        .format(to_pct_fmt)\
        .format(to_int_fmt, subset=['months'])\
        .format(to_float_fmt,
                subset=['calmar', 'monthly_skew', 'monthly_sharpe'])\
        .render()

    return html
