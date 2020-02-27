def isYearLeap(year):
#
# your code from LAB 4.1.3.6
#
    if (year % 100 == 0):
        if (year % 400 == 0):
            return True
        else:
            return False
    else:
        if (year % 4 == 0):
            return True
        else:
            return False


def daysInMonth(year, month):
#
# put your new code here
#
    if (month <= 7):
        if (month == 2):
            if (isYearLeap(year)):
                numdays=29
            else:
                numdays=28
        else:
            numdays=30+month%2
    else:
        numdays=31-month%2
    return numdays



def dayOfYear(year, month, day):
#
# put your new code here
#
    sum=0
    for i in range(1, month):
        sum+=daysInMonth(year,month=i)
    return(sum+day)


testYears = [1900, 2000, 2016, 1987]
testMonths = [2, 2, 1, 11]
testResults = [28, 29, 31, 30]
for i in range(len(testYears)):
	yr = testYears[i]
	mo = testMonths[i]
	print(yr, mo, "->", end="")
	result = daysInMonth(yr, mo)
	if result == testResults[i]:
		print("OK")
	else:
		print("Failed")

print('2000/12/31',dayOfYear(2000, 12, 31))
print('1900/12/31',dayOfYear(1900, 12, 31))
print('2001/2/10',dayOfYear(2001, 2, 10))

for year in range(0,2021,1):
    if isYearLeap(year):
        result='a Leap'
    else:
        result='Not a leap'
    print(year, "is ",result ," year")
