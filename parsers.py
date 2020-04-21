import os
import pandas as pd
import bt
import operator
import math

from data_loader import spreadsheets, asset_classes, data_dir, name_map
from itertools import accumulate, islice, combinations, chain


def adjust(acc, val):
    val = 0 if math.isnan(val) else val
    return acc * (1 + (val/100))


def adjust_pct(acc, val):
    val = 0 if math.isnan(val) else val
    return acc * (1 + val)


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


def eureka_to_date(d):
    m, y = d.split(' ')
    return pd.to_datetime(f"{y}-{m}-01")


def parse_eureka(fn):
    file = data_dir + fn
    ticker = generate_ticker(os.path.splitext(fn)[0])
    orig_data = pd.read_excel(file, skiprows=4, header=None,
                              index_col=0, names=['return', 'value'], parse_dates=False)
    dates = orig_data.index.map(eureka_to_date)
    data = pd.Series(orig_data['value'].mul(10), index=dates, name=ticker)

    return (ticker, data)


def load_all():
    return dict(chain(map(parse_excel, spreadsheets['excels']),
                      map(parse_tabular_csv, spreadsheets['tabular_csvs']),
                      map(parse_csv, spreadsheets['csvs']),
                      map(parse_amundi, spreadsheets['amundi']),
                      map(parse_eureka, spreadsheets['eurekahedge'])
                      )
                )
