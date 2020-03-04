def fun(inp=2, out=3):
    return inp * out

def fun1(x):
    if x % 2 == 0:
        return 1
    else:
        return

def fun2(inp=2, out=3):
    return inp * out

def any():
    print(var +1, end='')

def f(x):
    if x == 0:
        return 0
    return x + f(x-1)

def func1(a):
    return a ** a

def func2(a):
    return func1(a) * func1(a)

def fun(x):
    x += 1
    return x


dct={ }

lst=['a','b','c','d']

for i in range(len(lst) - 1):
    dct[lst[i]] = (lst[i],)

for i in (sorted(dct.keys())):
    k = dct[i]
    print(k[0])


x=2
x = fun(x+1)
print(x)

def fun(x,y,z):
    return x + 2 * y + 3 * z

def func(a,b):
    return a ** a

def func(x=0):
    return x

def func(x):
    global y
    y = x * x
    return y

tuple = (1,2)

tuple = (tuple[1],tuple[0])

print(tuple)

print(fun(0, z=1, y=3))

print(func(2))
print(y)
