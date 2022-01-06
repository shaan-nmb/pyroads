import numpy as np
import pandas as pd 

#From a start point, generate a deterioration curve based on a given rate and intervals to compound by.
def deteriorate(initial, rate,  final = False, n_periods = False, n_intervals = 1, percentage = False):
    
    if percentage:
        factor = 100
    else:
        factor = 1

    interval_rate = (1 + rate/factor)**(1/n_intervals) - 1

    l = [initial]

    if n_periods and final: 
        while l[-1] >= final and len(l) <= (n_intervals*n_periods):
            val = l[-1]*(1 - interval_rate)
            l.append(val)
    if n_periods and not final:
        while len(l) <= (n_intervals*n_periods):
            val = l[-1]*(1 - interval_rate)
            l.append(val)
    if final and not n_periods:
        while l[-1] >= final:
            val = l[-1]*(1 - interval_rate)
            l.append(val)

    return l

    
#define aggregation methods
def most(x):
    if len(x.mode()):
        return x.mode()[0] 
    else:
        return np.nan
        
def first(x):
    return x.iloc[0]

def last(x):
    return x.iloc[-1]  

def q75(x):
    return x.quantile(0.75)

def q90(x):
    return x.quantile(0.90)

def q95(x):
    return x.quantile(0.95)