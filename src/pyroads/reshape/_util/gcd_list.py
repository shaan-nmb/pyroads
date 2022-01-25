def gcd_list(items):
    items = list(items)

    def gcd(a, b):
        while b > 0:
            a, b = b, a % b
        return a

    result = items[0]

    for i in items[1:]:
        result = gcd(result, i)
    
    return result