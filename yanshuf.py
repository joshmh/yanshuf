import os
import pandas as pd
import bt
import operator
import math

from data import spreadsheets, asset_classes, data_dir, name_map
from itertools import accumulate, islice, combinations, chain
from monthdelta import monthmod

long_vol_combos = chain(combinations(
    asset_classes['long_vol'], 2), combinations(asset_classes['long_vol'], 1))


def adjust(acc, val):
    val = 0 if math.isnan(val) else val
    return acc * (1 + (val/100))


def adjust_pct(acc, val):
    val = 0 if math.isnan(val) else val
    return acc * (1 + val)


def to_series(d, key):
    l = islice(accumulate(d[key],
                          func=adjust, initial=1000), 1, None)
    return pd.Series(l, d.index)


def csv_date_parser(l):
    return "2010-01-01"


def combine_to_date(year, month):
    date = f'{year:04d}-{month:02d}-01'
    return pd.to_datetime(date)


def combine_to_date2(year, month):
    date = f'{year}-{month}-01'
    return pd.to_datetime(date)


def generate_ticker(file_base):
    return name_map[file_base] if file_base in name_map else file_base


def parse_csv(fn):
    file = data_dir + fn
    ticker = generate_ticker(os.path.splitext(fn)[0])
    df = pd.read_csv(file, skiprows=1, header=None,
                     names=["year", "month", ticker, "value"],
                     usecols=["year", "month", ticker], parse_dates=None)

    dates = df['year'].combine(df['month'], combine_to_date)
    l = islice(accumulate(df[ticker], func=adjust, initial=1000), 1, None)
    return (ticker, pd.Series(l, index=dates))


def parse_amundi(fn):
    file = data_dir + fn
    ticker = generate_ticker(os.path.splitext(fn)[0])
    df = pd.read_csv(file, skiprows=15, header=0,
                     names=['currency', 'value', 'dummy'],
                     index_col=1,
                     parse_dates=True)
    return (ticker, pd.Series(df['value']).mul(10))


def parse_tabular_csv(fn):
    file = data_dir + fn
    ticker = generate_ticker(os.path.splitext(fn)[0])
    df = pd.read_csv(file, skiprows=1, header=None, parse_dates=None,
                     names=['year', '01', '02', '03', '04', '05', '06',
                            '07', '08', '09', '10', '11', '12', 'ytd'],
                     usecols=['year', '01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12'])
    df = df.melt(id_vars=['year'], var_name='month', value_name=ticker)
    dates = df['year'].combine(df['month'], combine_to_date2)
    series = pd.Series(df[ticker].to_list(), index=dates)
    df.insert(0, 'date', dates)
    df.set_index('date', drop=True, inplace=True)
    df.sort_index(inplace=True)
    df.dropna(inplace=True)
    l = islice(accumulate(df[ticker], func=adjust, initial=1000), 1, None)
    return (ticker, pd.Series(l, index=df.index))


def parse_excel(fn):
    file = data_dir + fn
    ticker = generate_ticker(os.path.splitext(fn)[0])
    orig_data = pd.read_excel(file, skiprows=2, header=None,
                              index_col=0, names=[ticker], parse_date=False)
    l = islice(accumulate(orig_data[ticker],
                          func=adjust_pct, initial=1000), 1, None)
    data = pd.Series(l, orig_data.index)

    return (ticker, data)


data = dict(chain(map(parse_excel, spreadsheets['excels']),
                  map(parse_tabular_csv, spreadsheets['tabular_csvs']),
                  map(parse_csv, spreadsheets['csvs']),
                  map(parse_amundi, spreadsheets['amundi'])))

algo_stacks = [
    ('qr', [bt.algos.RunQuarterly(),
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

sp500 = parse_csv('sp-500.csv')[1]

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

    stats_series = res.stats[strategy_name]
    delta = res.stats[strategy_name].at['end'] - \
        res.stats[strategy_name].at['start']
    months_rec = monthmod(stats_series.at['start'], stats_series.at['end'])
    months = months_rec[0].months

    filtered_stats.loc['correlation'] = [corr]
    filtered_stats.loc['months'] = [months]

    return filtered_stats


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


df = run_all(long_vol_combos)
score = df.agg(long_vol_score, axis=1)
score.name = 'score'
df = df.join(score)
df.sort_values('score', inplace=True, ascending=False)
df.style.format({'score': '{:.2%}'})
print(df)

# res = backtest(quarterly_strategy, ['sp-500'], data)
# res = backtest(s1, list(long_vol_combos)[0], data)
# print(res)
# excel_dict = dict(map(parse_excel, excels))


# print(parse_amundi(spreadsheets['amundi'][0]))

# all.dropna(inplace=True)
# # print(all)

# s = bt.Strategy('s1', [bt.algos.RunQuarterly(),
#                        bt.algos.SelectAll(),
#                        bt.algos.WeighInvVol(),
#                        bt.algos.Rebalance()])

# test = bt.Backtest(s, all)
# res = bt.run(test)
# res.display()
