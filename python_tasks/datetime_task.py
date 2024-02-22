# Databricks notebook source

from datetime import date,datetime
 
def cal_Age(birthDate):
    today = date.today()
    age = today.year - birthDate.year - ((today.month, today.day) < (birthDate.month, birthDate.day))
 
    return age

def remaining_days_until_birthday(birthdate):
    today = datetime.now().date()
    next_birthday = datetime(today.year, birthdate.month, birthdate.day).date()

    # Check if birthday has already occurred this year
    if today > next_birthday:
        next_birthday = datetime(today.year + 1, birthdate.month, birthdate.day).date()

    # Calculate the remaining days
    remaining_days = (next_birthday - today).days
    
    return remaining_days

def months_and_weeks_passed(birthdate):
    today = datetime.now().date()
    delta = today - birthdate

    # Calculate the number of months and weeks
    months_passed = delta.days // 30     #Assuming an average month has 30 days
    weeks_passed = delta.days // 7

    return months_passed, weeks_passed
     
birth_date= input("Enter the birthddte (yyyy-mm-dd)")
birth_date_parts = map(int, birth_date.split('-'))
birth_date = date(*birth_date_parts)

print(f"Age: {cal_Age(birth_date)} years")

remaining_days = remaining_days_until_birthday(birth_date)
print(f"Remaining days until the birthday: {remaining_days} days")

months, weeks = months_and_weeks_passed(birth_date)
print(f"Number of months passed: {months} months")
print(f"Number of weeks passed: {weeks} weeks")


# COMMAND ----------



# COMMAND ----------


