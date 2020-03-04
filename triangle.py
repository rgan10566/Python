def isItATriangle(a, b, c):
    return a + b > c and b + c > a and c + a > b


def isItRightTriangle(a, b, c):
    if not isItATriangle(a, b, c):
        return False
    if c > a and c > b:
        return c ** 2 == a ** 2 + b ** 2
    elif a > b and a > c:
        return a ** 2 == b ** 2 + c ** 2
    else: return b ** 2 == a ** 2 + c ** 2

def heron(a, b, c):
    p = (a + b + c) / 2
    return (p * (p - a) * (p - b) * (p - c)) ** 0.5

def fieldOfTriangle(a, b, c):
    if not isItATriangle(a, b, c):
        return None
    return heron(a, b, c)


print(isItATriangle(1, 1, 1))
print(isItRightTriangle(1,1,1))
print(isItRightTriangle(5, 3, 4))
print(isItRightTriangle(1, 3, 4))
print(fieldOfTriangle(1., 1., 2. ** .5))
