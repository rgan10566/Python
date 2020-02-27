def isPrime(num):
#
# put your code here
#
    prime=True
    for j in range(2,num):
        if ( num%j == 0):
            prime=False
            break

    return prime

for i in range(1, 2000):
	if isPrime(i + 1):
			print(i + 1, end=" ")
print()
