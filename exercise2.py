#sample exercise on range for lists
#difference between  range and range(start,end)
#ranges from 0 thru 2
lst1=[i for i in range(3)]
print(lst1)
#ranegs from 1 thru 2 (3-1)
lst2=[i for i in range(1,3)]
print(lst2)
#nested lists
lst3=[[i for i in range(3)] for j in range(3)]
print(lst3)
sum=0
for i in range(3):
    sum+=lst3[i][i]
print(sum)
