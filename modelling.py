
#From a start point, generate a deterioration curve based on a given rate and intervals to compound by.
def deteriorate(start, end_at = None, n_periods, rate, n_intervals = 1, percentage = False):
    
    if percentage:
        factor = 100
    else:
        factor = 1

    interval_rate = (1 + rate/factor)**(1/n_intervals)

    l = [start]
    while l[-1] >= end_at or len(start) <= (n_intervals*n_periods):
        val = l[-1]*(1-rate)
        l.append(val)


    return l

    
