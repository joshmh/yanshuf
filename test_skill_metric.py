# September  2001  until  May  2017
import math
import empyrical as emp
from scipy import stats
import pandas as pd
import parsers
from skill_metric import skill_metric

annualized_frac = math.sqrt(12)

fund_name = 'HFRIEHI'
# fund_name = 'hfrifimb' # note: doesn't quite work, try some more hfr data to dial in mu
# fund_name = 'HFRIFOFD'
risk_free = 0.01 / 12   # Calculated from paper
series = parsers.load([fund_name])[fund_name].pct_change().loc['2001-09-01':'2017-05-01']
rr = series.mean() * 12 
print(('rr', rr))
print(('diff', rr - 0.0241))
adjusted = series.subtract(risk_free)
print(adjusted)

skew = adjusted.skew() / annualized_frac
sigma = adjusted.std() * annualized_frac
mu = adjusted.mean() * 12
tau = skill_metric(mu, sigma, skew)
tau_annualized = tau * math.sqrt(12)
kurt = adjusted.kurt()

print(('mu', mu))
print(('sigma', sigma))
print(('skew', skew))
print(('skew2', stats.skew(adjusted)/annualized_frac ))
print(('tau', tau))
print(('tau-annualized', tau_annualized))
print(('sharpe', emp.sharpe_ratio(series, risk_free, emp.MONTHLY)))
print(('kurt', kurt))

# tau is correct
# Pretty close for 3 funds, good enough
