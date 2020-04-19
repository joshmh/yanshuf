import os
import pandas as pd
import bt
import operator
import math
from data import spreadsheets, asset_classes
from itertools import accumulate, islice, combinations


long_vol_combos = combinations(asset_classes['long_vol'], 2)

print(list(long_vol_combos))


def adjust(acc, val):
    val = 0 if math.isnan(val) else val
    return acc + (acc * (val/100))


def to_series(d, key):
    l = islice(accumulate(d[key],
                          func=adjust, initial=1000), 1, None)
    return pd.Series(l, d.index)


def csv_date_parser(l):
    return "2010-01-01"


def combine_to_date(year, month):
    date = f'{year:04d}-{month:02d}-01'
    return pd.to_datetime(date)


def parse_csv(fn):
    dir = "/Users/josh/Tresors/seb-portfolio/portfolio/spreadsheets/"
    file = dir + fn
    ticker = os.path.splitext(fn)[0]
    df = pd.read_csv(file, skiprows=1, header=None,
                     names=["year", "month", ticker, "value"],
                     usecols=["year", "month", ticker], parse_dates=None)

    dates = df['year'].combine(df['month'], combine_to_date)
    l = islice(accumulate(df[ticker], func=adjust, initial=1000), 1, None)
    return pd.Series(l, index=dates)


def parse_excel(fn):
    dir = "/Users/josh/Tresors/seb-portfolio/portfolio/spreadsheets/"
    file = dir + fn
    ticker = os.path.splitext(fn)[0]
    orig_data = pd.read_excel(file, skiprows=2, header=None,
                              index_col=0, names=[ticker], parse_date=False)
    data = to_series(orig_data, ticker)
    return (ticker, data)


# excel_dict = dict(map(parse_excel, excels))


print(parse_csv(spreadsheets['csvs'][0]))

# all.dropna(inplace=True)
# # print(all)

# s = bt.Strategy('s1', [bt.algos.RunQuarterly(),
#                        bt.algos.SelectAll(),
#                        bt.algos.WeighInvVol(),
#                        bt.algos.Rebalance()])

# test = bt.Backtest(s, all)
# res = bt.run(test)
# res.display()
