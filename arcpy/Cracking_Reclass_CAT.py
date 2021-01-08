def reclass(a):
    if ( a >= 0 and a <0.01):
        return 1
    elif ( a>= 0.01 and a <0.03):
        return 2
    elif (a >= 0.03 and a <0.07):
        return 3
    elif (a >=0.07 and a < 0.15):
        return 4
    elif (a >= 0.15):
        return  5
    else:
        pass
