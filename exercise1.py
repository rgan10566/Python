#list difference between assignment and copy
#displays also using negative indexes in list

lst1=[1,2,3]
lst2=lst1
lst3=lst1[-3:-1]
print(lst1)
print(lst2)
print(lst3)
lst2.insert(0,4)
lst3.insert(0,4)
print(lst1)
print(lst2)
print(lst3)
#this should print the last number in the list
print(lst1[-1])
#this prints from the last but third element till the last element (not including the last element)
print(lst1[-3:-1])
#this prints from the first number till the last element (not including the last element)
print(lst1[0:-1])
