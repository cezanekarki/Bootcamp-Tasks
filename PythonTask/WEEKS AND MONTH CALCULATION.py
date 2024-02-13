from datetime import datetime

def months_weeks_since_birth(date_of_birth):
    # Assuming date_of_birth is in the format 'YYYY-MM-DD'
    dob = datetime.strptime(date_of_birth, '%Y-%m-%d')
    current_date = datetime.now()
    time_difference = current_date - dob
    
    # Calculate the number of months and weeks
    months = time_difference.days // 30  # Assuming an average month has 30 days
    weeks = time_difference.days // 7
    
    return months, weeks

date_of_birth = '1999-01-12'
months_passed, weeks_passed = months_weeks_since_birth(date_of_birth)

print(f"{months_passed} months and {weeks_passed} weeks have passed since the person was born.")

