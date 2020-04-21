import pandas as pd
import bt
import data_loader

from itertools import islice
from monthdelta import monthmod
from parsers import load_all

algo_stacks = [
    ('qre', [bt.algos.RunQuarterly(),
             bt.algos.SelectAll(),
             bt.algos.WeighEqually(),
             bt.algos.Rebalance()]
     )
]

stat_keys = ['max_drawdown', 'monthly_vol', 'best_month', 'best_year', 'worst_month',
             'worst_year', 'monthly_skew', 'monthly_sharpe', 'cagr', 'calmar']


def backtest(algo_stack, keys, data):
    filtered_data = {k: v for k, v in data.items() if k in keys}
    df = pd.DataFrame(filtered_data)
    df.dropna(inplace=True)
    # print(df)
    strategy_name = ':'.join(keys) + '@' + algo_stack[0]
    s = bt.Strategy(strategy_name, algo_stack[1])

    test = bt.Backtest(s, df, progress_bar=False)
    res = bt.run(test)
    corr = df.corr().iat[0, 1] if df.shape[1] == 2 else None
    filtered_stats = res.stats.filter(
        stat_keys, axis='index')

    ss = res.stats[strategy_name]
    months_rec = monthmod(ss['start'], ss['end'])
    months = months_rec[0].months

    return_table = res[strategy_name].return_table

    extras = pd.Series({
        'correlation': corr,
        'explosivity': ss['best_month'] / (2*-ss['max_drawdown']),
        'mar-2020': return_table.at[2020, 'Mar'],
        '2020': return_table.at[2020, 'YTD'],
        '2019': return_table.at[2019, 'YTD'],
        '2018': return_table.at[2018, 'YTD']
    }, name=strategy_name)

    # final = filtered_stats.merge(extras)
    final = filtered_stats.append(extras)
    print(extras)
    print(final)
    return final


data = load_all()


def run_all(keylists):
    first = True
    for keylist in keylists:
        for algo_stack in algo_stacks:
            df_new = backtest(algo_stack, keylist, data)
            df = df_new if first else df.join(df_new)
            first = False
    return df.transpose()


def long_vol_score(df):
    dd = df[0]
    return dd


def to_float_fmt(f):
    return "None" if f == None else f"{f:.3f}"


def to_pct_fmt(f):
    return "None" if f == None else f"{f:.2%}"


def to_int_fmt(f):
    return f'{f}'


def make_formatter(columns):
    isnum = ['score', 'correlation', 'calmar', 'months']
    return list(map(lambda x: to_float_fmt if x in isnum else to_pct_fmt, columns))


long_vol_groups = data_loader.long_vol_groups()

df = run_all(long_vol_groups)
score = df.agg(long_vol_score, axis=1)
score.name = 'score'
df = df.join(score)
# df.sort_values('explosivity', inplace=True, ascending=False)

# html = df.style\
#     .format(to_pct_fmt)\
#     .format(to_float_fmt, subset=[
#         'score', 'correlation', 'calmar', 'months', 'explosivity', 'monthly_skew', 'monthly_sharpe'])\
#     .format(to_int_fmt, subset=['months'])\
#     .render()
# print(html)
