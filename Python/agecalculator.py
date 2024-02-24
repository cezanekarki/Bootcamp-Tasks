from datetime import datetime

def calculate_age(dob):
    today = datetime.today()
    age = today.year - dob.year - ((today.month, today.day) < (dob.month, dob.day))
    return age

def remaining_days_until_birthday(birthdate):
    today = datetime.today()
    next_birthday = datetime(today.year, birthdate.month, birthdate.day)
    if next_birthday < today:
        next_birthday = datetime(today.year + 1, birthdate.month, birthdate.day)
    days_until_birthday = (next_birthday - today).days
    return days_until_birthday

def months_and_weeks_passed_since_birth(birthdate):
    today = datetime.today()
    age = today - birthdate
    
    months_passed = age.days // 30  # Approximate number of days in a month
    
    weeks_passed = age.days // 7
    
    return months_passed, weeks_passed

def main():
    birthdate_str = input("Enter your birthdate (YYYY-MM-DD): ")
    birthdate = datetime.strptime(birthdate_str, '%Y-%m-%d')

    age = calculate_age(birthdate)
    print("Your age is:", age)

    remaining_days = remaining_days_until_birthday(birthdate)
    print("Remaining days until birthday:", remaining_days)

    months_passed, weeks_passed = months_and_weeks_passed_since_birth(birthdate)
    print("Number of months passed since birth:", months_passed)
    print("Number of weeks passed since birth:", weeks_passed)

if __name__ == "__main__":
    main()