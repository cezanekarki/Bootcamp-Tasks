"""
Bootcamp Task: 

Project: Contact Book Application 

Objective: Create a Python program that allows users to manage their contacts. Users should be able to add, view, search for, and delete contacts. 


Project Steps: 

Initialize the Contact Book: 

Create an empty list to represent the contact book. Each contact will be a dictionary with attributes like name, phone number, and email. 

Create Functions: 

Define functions for the following actions: 

Add a contact. 

View all contacts. 

Search for a contact by name. 

Delete a contact by name. 

Main Menu: 

Implement a loop that displays a menu of options for the user. 

Options may include adding a contact, viewing contacts, searching for a contact, deleting a contact, or quitting the program. 

User Input: 

Depending on the user's choice from the menu, prompt them for the necessary input (e.g., name, phone number). 

Perform Actions: 

Based on the user's choice, call the corresponding function to perform the action. 

Display the Updated Contact List: 

After each action, display the updated contact list to the user. 

Exit: 

Allow the user to exit the program when they are done managing their contacts. 

Hints: 

Use a list to store the contact dictionaries. 

Each contact can be represented as a dictionary with keys like "name," "phone," and "email." 

Create functions for adding, viewing, searching, and deleting contacts. 

Implement input validation to handle different user inputs. 

Utilize a lambda function for searching contacts based on name. 

Use *args and **kwargs in functions to handle variable numbers of arguments for contact attributes. 
"""

import time
import re
class ContactBookApplication():
    def __init__ (self):
        self.contact_list = []

    def add_contact(self,**kwargs):
        # keys = ["name","phone","email"]
        # values = {key: kwargs.get(key,"") for key in["name","phone","email"]}
        # name, phone, email = values["name"], values["phone"], values["email"]

        name = kwargs.get("name", "")
        phone = kwargs.get("phone", "")
        email = kwargs.get("email", "")
        if not Validator.validate_name(name):
            print("Please enter the valid name")
            return
        if not Validator.validate_phone(phone):
            print("Please enter the valid phone")
            return
        if not Validator.validate_email(email):
            print("Please enter the valid email")
            return

        contact_info = {
            "name":name,
            "phone":phone,
            "email":email
        }
        self.contact_list.append(contact_info)
        print(f"Contact is added succesfully")
        print(f"The added information are:{self.contact_list}")
       

    def get_all_contact(self):
        if not self.contact_list:
            print(f"The list is empty please add some records")
        else:
            for contact in self.contact_list:
                print(f"Availble information in the Contact Book Application: {contact}")
    
    def get_contact_by_name(self,name):
        searched_information = list(filter(lambda data:data["name"] == name,self.contact_list))
        if  searched_information:
            print(f"The list for the {name} is {searched_information}")
        else:
            print("No result found for {name}")

    
    def delete_contact_by_name(self,name):
        if self.contact_list:
            self.contact_list = [contact for contact in self.contact_list if contact['name'] != name]
            print(f"Contact {name} deleted successfully!")
        else:
            print(f"No available information for the {name}")


class MenuHandler:
    @staticmethod
    def display_menu():
        print("\n===== Contact Book Menu =====")
        print("\n***** Choice Menu ****")
        print("1. Add Contact Informations")
        print("2. View all Contact Informations")
        print("3. Search Contact by name")
        print("4. Delete Contact by name")
        print("5. Quit")
        print("*******************************")

    @staticmethod
    def get_user_input(prompt):
        return input(prompt)
    
class Validator:
    @staticmethod
    def validate_name(name):
        return name.strip() and  all(char.isalpha() for char in name) and (len(name) <= 10)
    @staticmethod
    def validate_phone(phone):
        return phone.isdigit() and len(phone) == 10 
    
    @staticmethod
    def validate_email(email):
        email_pattern = re.compile(r"[^@]+@[^@]+\.[^@]+")
        return bool(re.match(email_pattern, email))


def main_menu(contact_book, menu_handler):
    while True:
        menu_handler.display_menu()
        choice = menu_handler.get_user_input("Enter your choice from the menu: ")

        if choice == '1':
            name = menu_handler.get_user_input("Enter the name: ")
            phone = menu_handler.get_user_input("Enter the phone number: ")
            email = menu_handler.get_user_input("Enter the email address: ")
            contact_book.add_contact(name=name,phone=phone,email=email)
        elif choice == '2':
            contact_book.get_all_contact()

        elif choice == '3':
            name = menu_handler.get_user_input("Enter the name to search: ")
            contact_book.get_contact_by_name(name)
          
        elif choice == '4':
            name = menu_handler.get_user_input("Enter the name to delete: ")
            contact_book.delete_contact_by_name(name)
           
        elif choice == '5':
            print("Exiting..........")
            time.sleep(1)
            print("Thank you for using Book Application.Goodbye!")
            break
        else:
            print("Invalid choice. Please enter a number between 1 and 5.")

if __name__ == "__main__":
    contact_book = ContactBookApplication()
    menu_handler = MenuHandler()
    main_menu(contact_book, menu_handler)
