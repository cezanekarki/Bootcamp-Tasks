from datetime import datetime


def calculate_age(date_of_birth):
    # Assuming date_of_birth is in the format 'YYYY-MM-DD'
    dob = datetime.strptime(date_of_birth, '%Y-%m-%d')

    current_date = datetime.now()
    # Calculate the age
    age = current_date.year - dob.year - ((current_date.month, current_date.day) < (dob.month, dob.day))    
    return age


date_of_birth = '1999-01-12'
age = calculate_age(date_of_birth)
print(f"The person is {age} years old.")




