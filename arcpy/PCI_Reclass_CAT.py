def reclass(a):
    if ( a >= 0 and a <1.5):
        return "Very Poor"
    elif ( a>= 1.5 and a < 2.5):
        return "Poor"
    elif (a >= 2.5 and a < 3.5):
        return "Fair"
    elif (a >=3.5 and a < 4.5):
        return "Good"
    elif (a >= 4.5):
        return  "Very Good"
    else:
        pass
