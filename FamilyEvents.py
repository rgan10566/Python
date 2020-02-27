from datetime import date
Events=('Birth','Marriage','Death','School','Divorce','Job')

def get_events_year(d, yr):
    ydict={}
    if type(d) is dict:
        for k,v in d.items():
            if type(v) is dict:
                ydict.update(get_events_year(v,yr))
            elif type(v) is list:
                if v[2] == yr:
                    ydict.update({v[2]:v})
    return ydict




## Ramesh Ganesan Events
## Evens in Ramesh Ganesan's Life
rgEvents = {
0: [10, 5, 1966, Events[0],'Birth', 'Born in Gosha Government Hospital in Triplicane, Chennai, TN, India'],
1: [24,9,1996,Events[1],'Got wedded to Annu in chennai at Meenakshi Kalyana Mandapam in T.Nagar, Chennai, TN, India'],
2: [1,10,2009,Events[5],'Got divorced']
}

## Akash Ramesh Events
## Evens in Akash Ramesh's Life
arEvents={
0:[1,10,1997,Events[0],'Born in Viajaya Hospital Vadapalani, Chennai, TN, India']
}

## Tanya Ramesh Events
## Evens in Tanya Ramesh's Life
trEvents={
0 : [7,1,2001,Events[0],'Born in Torrance Memorial Hospital, Torrance, CA, USA']
}


rg_famevents={
'Ramesh': rgEvents,
'Akash': arEvents,
'Tanya': trEvents
}

sg_famevents={'Ramesh':rgEvents}

#print(rgEvents)
#print(rg_famevents)
#yd1=get_events_year(rg_famevents,1997)
#print(sg_famevents)
#print(yd1)
#yd2=get_events_year(rgEvents,1997)
#print(yd2)
print(get_events_year(rg_famevents,1997),get_events_year(rg_famevents,1996))
