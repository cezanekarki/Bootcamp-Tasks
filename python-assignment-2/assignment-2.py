"""
write a python function to calculate someone's age.
write another functionto calculate remaing days left for the person's birthday.
write another function to calculate the number of months and weeks passed after the person was born.
"""

from datetime import datetime


# 1. write a python function to calculate someone's age.
def age_calc(dob):
    birthday_format = datetime.strptime(dob,'%Y-%m-%d')
    today_date = datetime.now()
    age = today_date.year - birthday_format.year
    return age
#input format YYYY-MM-DD
person_age = age_calc('1998-12-31')
print(f"The person's age according to provided birthday is {person_age} ")

# 2. write another function to calculate remaing days left for the person's birthday.

def remaining_days(dob):
    birthday_format = datetime.strptime(dob,'%Y-%m-%d')
    today_date = datetime.now()
    #logic
    next_birthday = datetime(today_date.year,birthday_format.month,birthday_format.day)
    if today_date > next_birthday:
        next_birthday = datetime(today_date.year + 1,birthday_format.month,birthday_format.day)
    days_remaining = (next_birthday - today_date).days
    return days_remaining
print(remaining_days('1998-1-31'))

# 3. write another function to calculate the number of months and weeks passed after the person was born.
def month_week_pass(dob):
    birthday_format = datetime.strptime(dob, '%Y-%m-%d')
    today_date = datetime.now()

    time_diff = today_date - birthday_format  # this returns a timedelta object

    # Calculate the number of months and weeks accurately
    total_days = time_diff.days
    months_passed = total_days // 30
    weeks_passed = total_days // 7
    
    return months_passed, weeks_passed

month, week = month_week_pass('1998-01-31')
print(f'The number of months passed is {month} and weeks passed is {week}')