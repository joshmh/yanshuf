# September  2001  until  May  2017
import math
import empyrical as emp
from scipy import stats
import pandas as pd
import parsers
from skill_metric import skill_metric

annualized_frac = math.sqrt(12)

sp_name = 'sp500'
risk_free = 0.7 / 365   # Approximate daily yield of 1-month treasures for end of range
series = parsers.load([sp_name])[sp_name].pct_change().loc['2001-09-01':'2017-05-01']
print(series)
adjusted = series.subtract(risk_free)
skew = adjusted.skew() / annualized_frac
sigma = adjusted.std()
mu = adjusted.mean()

skew_f = skew

tau = skill_metric(mu, sigma, skew_f)
tau_annualized = tau * math.sqrt(12)

print(('skew', skew))
print(('skew2', stats.skew(adjusted)/annualized_frac ))
print(('tau', tau))
print(('tau-annualized', tau_annualized))
print(('sharpe', emp.sharpe_ratio(series, risk_free, emp.MONTHLY)))

# tau-annualized is correct
# note: for some reason skew is a bit different from paper