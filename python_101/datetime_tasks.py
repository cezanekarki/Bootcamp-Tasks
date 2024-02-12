# Task 1: calculate age using dob
from datetime import datetime

def calculate_age(dob):
    today = datetime.today()
    # to offset the error when the birthday has not yet occurred
    if today.month < dob.month or (today.month == dob.month and today.day < dob.day):  #for birth month checking day
        return today.year - dob.year - 1
    else:
    
        return f'You are {today.year - dob.year} years old.' 

dob = datetime(2001, 8, 4)
print(dob)
print(calculate_age(dob))

#Task 2: Function to calculate reamaining days left for person's birthday
def days_until_birthday(birthday):
    #2024-02-01 to 2024-08-04
    
    today = datetime.today()
    if today.month > birthday.month:
        next_year = today.year + 1
    else:
        next_year = today.year
    next_bday = datetime(next_year,birthday.month, birthday.day)
    days = (next_bday-today).days
    return f'Your birthday is {days} days away.'

bday = datetime(2001,8,4)
# bday = datetime(2001,2,13)
# bday = datetime(2001,1,4)

print(days_until_birthday(bday))

#Task 3: FN TO CALCULATE NO OF MONTHS AMD WEEKS PASSED AFTER THE PERSON WAS BORN
def week_month(birthday):
    today = datetime.today().date()
    years = (today- birthday).days
    return f'You have spent {years//30} months and {years//7} weeks in this beautiful planet.'

bday = datetime(2001,8,4).date()


print(week_month(bday))

