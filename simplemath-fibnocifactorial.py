def factorial(n):
    if n < 0:
        return None
    elif n == 0:
        return 1
    else:
        return n*factorial(n-1)

def fibonacci(n):
    if n < 0:
        return None
    elif n == 0:
        return 0
    elif n == 1:
        return 1
    else:
        return fibonacci(n-1)+fibonacci(n-2)

for i in range(11):
        print("Factorial", i, factorial(i))
        print("Fibonacci", i, fibonacci(i))
