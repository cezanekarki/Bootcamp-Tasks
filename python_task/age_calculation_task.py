
from datetime import datetime

def validate_date_format(date_of_birth):
    try:
        datetime.strptime(date_of_birth, '%Y-%m-%d')
        return True
    except ValueError:
        return False

def calculate_age(date_of_birth):
    if not validate_date_format(date_of_birth):
        raise ValueError("The input date format should look like this: 'YYYY-MM-DD'")

    birth_date = datetime.strptime(date_of_birth, '%Y-%m-%d')
    current_date = datetime.now()
    age = current_date.year - birth_date.year - ((current_date.month, current_date.day) < (birth_date.month, birth_date.day))
    return age

def remaining_days_for_birthday(date_of_birth):
    if not validate_date_format(date_of_birth):
        raise ValueError("The input date format should look like this: 'YYYY-MM-DD'")

    birth_date = datetime.strptime(date_of_birth, '%Y-%m-%d')
    current_date = datetime.now()
    next_birthday = datetime(current_date.year, birth_date.month, birth_date.day)

    if current_date > next_birthday:
        next_birthday = datetime(current_date.year + 1, birth_date.month, birth_date.day)

    days_until_birthday = (next_birthday - current_date).days
    return days_until_birthday


def months_weeks_passed_after_birth(date_of_birth):
    if not validate_date_format(date_of_birth):
        raise ValueError("The input date format should look like this: 'YYYY-MM-DD'")

    birth_date = datetime.strptime(date_of_birth, '%Y-%m-%d')
    current_date = datetime.now()
    total_days = current_date - birth_date
    months_passed = total_days.days // 30
    weeks_passed = total_days.days // 7

    return months_passed, weeks_passed


date_of_birth = '1992-11-15'

try:
    age = calculate_age(date_of_birth)
    print(f"You are {age} years old")

    days_until_birthday = remaining_days_for_birthday(date_of_birth)
    print(f"Your next birthday is in   {days_until_birthday} days")

    months_passed, weeks_passed = months_weeks_passed_after_birth(date_of_birth)
    print(f"  {months_passed}, months were passed after your birth")
    print(f"  {weeks_passed}, weeks were passed after your birth")

except ValueError as e:
    print(f"Error: {e}")