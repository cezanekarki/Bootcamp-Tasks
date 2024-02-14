from datetime import datetime

now = datetime.now()
def calculate_age(dob):
    age = now.year - dob.year
    print('Age is: ', age)
    
def upcoming_birthday(dob):
    month_diff = dob.month - now.month
    day_diff = dob.day - now.day
    if now.month < dob.month:
        remaining_days = month_diff * 30 + day_diff
        print('Remaining days for birthday: ', remaining_days)
    else:
        remaining_days = 365 - abs(month_diff * 30 + day_diff)
        print('Remaining days for birthday: ', remaining_days)
        
def count_week_and_month(dob):
    total_days = (now - dob).days
    total_weeks = total_days // 7
    total_months = (now.year - dob.year) * 12 + (now.month - dob.month)
    print('Total weeks are: ', total_weeks)
    print('Total months are: ', total_months)
    
dob = input("Enter your birthday in 'mm/dd/yyyy' format: \n")
formatted_dob = datetime.strptime(dob, '%m/%d/%Y')
calculate_age(formatted_dob)
upcoming_birthday(formatted_dob)
count_week_and_month(formatted_dob)
        
        
