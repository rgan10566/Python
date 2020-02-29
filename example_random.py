from random import randrange

def create_tuple():
    tuple = (1,2,5)
    return tuple

# for i in range(10):
#     print(randrange(8))
# for i in range(1,100) :
#     print(randrange(10))

tup=create_tuple()
count=0

while True:
    i = randrange(10)
    if i in tup:
        print("Random number ",i, "found in tuple")
        break
    else:
        print("Random number ",i,"not found in tupple")
        count=count+1

print("Attempts made", count)
#print(tup)
