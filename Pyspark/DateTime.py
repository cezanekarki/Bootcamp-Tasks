from datetime import datetime

def calculateAge(birthDate):
    today = datetime.today()
    year=today.year - birthDate.year
    age = year - ((today.month, today.day) < (birthDate.month, birthDate.day))
    print(f"\nAge is : {age}")
    return age

calculateAge(datetime(2000,9,14))


def calculateRemainingDayBirthday(birthDate):
    today = datetime.today()
    nextBirthday=datetime(today.year,birthDate.month,birthDate.day)
    if today>nextBirthday:
        nextBirthday=datetime(today.year+1,birthDate.month,birthDate.day)
    remainingDays=nextBirthday-today
    print(f"\nRemaining days for next birthday : {remainingDays.days}")
    return remainingDays.days

    
    
    
calculateRemainingDayBirthday(datetime(2000,9,14))


def calculateMonthsAndWeeks(birthDate):
    print(birthDate)
    today = datetime.today()
    yearPassed=today.year - birthDate.year # 2024-2000 = 24 
    monthsPassed=today.month - birthDate.month # 2-9 = 7
    monthPassed=yearPassed*12 + monthsPassed # 24*12 + 7 = 281
    weekPassed=monthPassed*4
    
    print(f"\n*****Months and weeks passed since birth*****")
    print(f"Months passed : {monthPassed}")
    print(f"Weeks passed : {weekPassed}")
    
    
calculateMonthsAndWeeks(datetime(2000,9,14))


