from math import pi, atan
from eta_table import ETA_TABLE
            
def sgn(x):
    if x < 0: return -1
    if x > 0: return 1
    return 0

def compute_eta(skew):
    return ETA_TABLE[round(skew, 2)]

def skill_metric(mu, sigma, skew):
    eta = compute_eta(skew)
    print(('eta', eta))
    t = (2 / pi) * ((eta ** 2) / (1 + eta ** 2))
    return (mu / sigma) * ((1 - t) ** 0.5) + \
        sgn(eta) * (2 / pi) * atan((2 * mu) / (pi * sigma)) * (t ** 0.5)

