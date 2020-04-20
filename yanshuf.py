import os
import pandas as pd
import bt
import operator
import math
from data import spreadsheets, asset_classes, data_dir
from itertools import accumulate, islice, combinations, chain


long_vol_combos = combinations(asset_classes['long_vol'], 2)


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


def parse_csv(fn):
    file = data_dir + fn
    ticker = os.path.splitext(fn)[0]
    df = pd.read_csv(file, skiprows=1, header=None,
                     names=["year", "month", ticker, "value"],
                     usecols=["year", "month", ticker], parse_dates=None)

    dates = df['year'].combine(df['month'], combine_to_date)
    l = islice(accumulate(df[ticker], func=adjust, initial=1000), 1, None)
    return (ticker, pd.Series(l, index=dates))


def parse_amundi(fn):
    file = data_dir + fn
    ticker = os.path.splitext(fn)[0]
    df = pd.read_csv(file, skiprows=15, header=0,
                     names=['currency', 'value', 'dummy'],
                     index_col=1,
                     parse_dates=True)
    return (ticker, pd.Series(df['value']).mul(10))


def parse_tabular_csv(fn):
    file = data_dir + fn
    ticker = os.path.splitext(fn)[0]
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
    ticker = os.path.splitext(fn)[0]
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


s = bt.Strategy('s1', [bt.algos.RunQuarterly(),
                       bt.algos.SelectAll(),
                       bt.algos.WeighInvVol(),
                       bt.algos.Rebalance()])


sp500 = parse_csv('sp-500.csv')[1]


def backtest(s, keys, data):
    filtered_data = {k: v for k, v in data.items() if k in keys}
    df = pd.DataFrame(filtered_data)
    df.dropna(inplace=True)
    # print(df)
    test = bt.Backtest(s, df, progress_bar=False)
    res = bt.run(test)
    prices = res.prices['s1']
    corr = sp500.corr(prices)

    print(f"Correlation S&P500: {corr:.3}")


print(sp500)
backtest(s, list(long_vol_combos)[0], data)
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
