# Implementation Using functional programming
import re

contacts = []

def validate_name(name):
    if not re.match(r'^[a-zA-Z]{3,}$', name):
        raise ValueError("Invalid name. At least 3 characters and contain only letters.")

def validate_phone(phone):
    if not re.match(r'^\d{10}$', phone):
        raise ValueError('Invalid phone number. Must be 10 digits.')

def validate_email(email):
    if not re.match(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$', email):
        raise ValueError('Invalid email address. Please enter a valid email.')

def add_contact(*args):
    try:
        name,phone,email = args
        validate_name(name)
        validate_phone(phone)
        validate_email(email)

        prev_len = len(contacts)
        contact = {"name": name, "phone": phone, "email": email}
        contacts.append(contact)
        if len(contacts) == prev_len:
            print('Contact failed to add.')
        else:
            print('Contact added successfully!')

    except ValueError as ve:
        print(f'Error: {ve}')

def view_contacts():
    if not contacts:
        print('No contacts found.')
    else:
        for contact in contacts:
            print('All contact list\n', contact)

def search_contact(search_name):
    try:
        validate_name(search_name)

        result = list(filter(lambda x: x['name'].lower() == search_name.lower(), contacts))
        if not result:
            print('Contact not found')
        else:
            print('Searched Result\n', result)
    except ValueError as ve:
        print(f'Error: {ve}')

def delete_contact(delete_name):
    try:
        validate_name(delete_name)
        global contacts
        prev_len = len(contacts)
        contacts = list(filter(lambda x: x['name'].lower() != delete_name.lower(), contacts))
        if len(contacts) == prev_len:
            print(f'Contact with name {delete_name} not found. No contact deleted')
        else:
            print('Contact deleted successfully!')
    except ValueError as ve:
        print(f'Error: {ve}')

def main_menu():
    while True:
        print('\n Contact Book Application')
        print('1. Add Contact')
        print('2. View Contacts')
        print('3. Search Contact')
        print('4. Delete Contact')
        print('5. Exit')

        try:
            choice = input('Enter your choice (1-5): ')

            # choice validate
            if not choice.isdigit or not 1 <= int(choice) <= 5:
                raise ValueError('Invalid choice. Please enter a number between 1 and 5.')

            if choice == '1':
                name = input('Enter your name: ')
                phone = input('Enter your phone number: ')
                email = input('Enter your email address: ')
                add_contact(name,phone,email)
            elif choice == '2':
                view_contacts()
            elif choice == '3':
                search_name = input('Enter the name to search:')
                search_contact(search_name)
            elif choice == '4':
                delete_name = input('Enter the name to delete the contact: ')
                delete_contact(delete_name)
            elif choice == '5':
                print('Exiting the program...')
                break
        except ValueError as e:
            print(f'Error: {e}')

if __name__ == '__main__':
    main_menu()
