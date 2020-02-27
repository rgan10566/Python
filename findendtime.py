hour = int(input("Starting time (hours): "))
mins = int(input("Starting time (minutes): "))
dura = int(input("Event duration (minutes): "))

# put your code here
minutes = (mins + dura) % 60
thours = ( hour + (mins + dura) // 60 ) % 24

print("End Time is "+str(thours)+":"+str(minutes))
