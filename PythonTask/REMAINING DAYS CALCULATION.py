from datetime import datetime

def remaining_days_until_birthday(date_of_birth):
    # Assuming date_of_birth is in the format 'YYYY-MM-DD'
    dob = datetime.strptime(date_of_birth, '%Y-%m-%d')
    
    current_date = datetime.now()
    
    # Calculate the next birthday
    next_birthday = datetime(current_date.year, dob.month, dob.day)
    
    # If the birthday has already occurred this year, calculate for the next year
    if current_date > next_birthday:
        next_birthday = datetime(current_date.year + 1, dob.month, dob.day)
    
    remaining_days = (next_birthday - current_date).days
    
    return remaining_days


date_of_birth = '1999-01-12'
days_until_birthday = remaining_days_until_birthday(date_of_birth)

if days_until_birthday == 0:
    print("Happy Birthday!")
else:
    print(f"There are {days_until_birthday} days left until the next birthday.")
