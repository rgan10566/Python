vals=[0,1,2]
vals.insert(0,1)
del vals[1]
print(vals)
sum=0
for i in range(len(vals)):
    sum+=vals[i]
print(sum)
