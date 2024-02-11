from datetime import datetime
import os
import time

print(datetime(2020,1,1))
print(datetime.now())
os.environ['TZ'] = 'Asia/kolkata'
time.tzset()
print(datetime.now())

#pytz
# astimezone => its use scenario
#utcnow()=> its use scenario
# strfTime
# strpTime=> for specifying the format
