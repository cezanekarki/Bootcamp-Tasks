"""
Write a python function to calculate someone's age using date of birth.
Write another function to calculate remaining days left for the person's birthday.
Write another function to calculate the number of months and weeks passed after the person was born.
"""

from datetime import datetime

def validate_date_format(date_of_birth):
    try:
        datetime.strptime(date_of_birth, '%Y-%m-%d')
        return True
    except ValueError:
        return False

def calculate_age(date_of_birth):
    if not validate_date_format(date_of_birth):
        raise ValueError("Please use 'YYYY-MM-DD' format for the calculation")

    birth_date = datetime.strptime(date_of_birth, '%Y-%m-%d')
    current_date = datetime.now()
    age = current_date.year - birth_date.year - ((current_date.month, current_date.day) < (birth_date.month, birth_date.day))
    return age

def remaining_days_for_birthday(date_of_birth):
    if not validate_date_format(date_of_birth):
        raise ValueError("Please use 'YYYY-MM-DD' format for the calculation")

    birth_date = datetime.strptime(date_of_birth, '%Y-%m-%d')
    current_date = datetime.now()
    next_birthday = datetime(current_date.year, birth_date.month, birth_date.day)

    if current_date > next_birthday:
        next_birthday = datetime(current_date.year + 1, birth_date.month, birth_date.day)

    days_until_birthday = (next_birthday - current_date).days
    return days_until_birthday


def months_weeks_passed_after_birth(date_of_birth):
    if not validate_date_format(date_of_birth):
        raise ValueError("Please use 'YYYY-MM-DD' format for the calculation")

    birth_date = datetime.strptime(date_of_birth, '%Y-%m-%d')
    current_date = datetime.now()

    total_days = current_date - birth_date
    months_passed = total_days.days // 30
    weeks_passed = total_days.days // 7

    return months_passed, weeks_passed


date_of_birth = '1971-09-17'

try:
    age = calculate_age(date_of_birth)
    print(f"You are {age} years old")

    days_until_birthday = remaining_days_for_birthday(date_of_birth)
    print(f"Your birthday will be in  {days_until_birthday} days")

    months_passed, weeks_passed = months_weeks_passed_after_birth(date_of_birth)
    print(f"Months Since you were born  {months_passed} months")
    print(f"Weeks  Since you were born  {weeks_passed} weeks")

except ValueError as e:
    print(f"Error: {e}")