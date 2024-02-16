# Implementation Using Object Oriented Programming
import re

class Contact:
    def __init__(self,name,phone,email):
        self.name = name
        self.phone = phone
        self.email = email

class ContactBook:
    def __init__(self) -> None:
        self.contacts = []
    def validate_name(self,name):
         if not re.match(r'^[a-zA-Z]{3,}$', name):
            raise ValueError("Invalid name. At least 3 characters and contain only letters.")
    def validate_phone(self, phone):
        if not re.match(r'^\d{10}$', phone):
            raise ValueError('Invalid phone number. Must be 10 digits.')
    def validate_email(self, email):
        if not re.match(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$', email):
            raise ValueError('Invalid email address. Please enter a valid email.')
    
    def add_contact(self,*args):
        try:
            name,phone,email = args
            self.validate_name(name)
            self.validate_phone(phone)
            self.validate_email(email)
            prev_len = len(self.contacts)
            #or can also directly append dict in contacts attribute without creating the Contact class instance
            contact = Contact(name,phone,email)
            self.contacts.append(contact)
            if len(self.contacts) == prev_len:
                print('Contact failed to add.')
            else:
                print('Contact added successfully!')
        except ValueError as e:
            print(f'Error: {e}')

    def view_contacts(self):
        if not self.contacts:
            print('No contacts found.')
        else:
            for contact in self.contacts:
                print( f"Name: {contact.name}, Phone: {contact.phone}, Email: {contact.email}")

    def search_contact(self,search_name):
        try:
            self.validate_name(search_name)
            result = list(filter(lambda x: x.name.lower() == search_name.lower(), self.contacts))
            if not result:
                print('Contact not found')
            else:
                for contact in result:
                    print(f"Name: {contact.name}, Phone: {contact.phone}, Email: {contact.email}")
        except ValueError as e:
            print(f'Error: {e}')

    def delete_contact(self, delete_name):
        try:
            self.validate_name(delete_name)
            prev_len = len(self.contacts)
            self.contacts = list(filter(lambda x: x.name.lower() != delete_name.lower(), self.contacts))
            if len(self.contacts) == prev_len:
                print(f'Contact with name {delete_name} not found. No contact deleted')
            else:
                print('Contact deleted successfully!')
        except ValueError as e:
            print(f'Error: {e}')


def main():
    contact_book = ContactBook()
    while True:
        print('\n Contact Book Application')
        print('1. Add Contact')
        print('2. View Contacts')
        print('3. Search Contact')
        print('4. Delete Contact')
        print('5. Exit')
        try:
            choice = input('Enter your choice (1-5): ')
            if not choice.isdigit() or not 1 <= int(choice) <= 5:
                raise ValueError('Invalid choice. Please enter a number between 1 and 5.')
            if choice == '1':
                name = input('Enter your name: ')
                phone = input('Enter your phone number: ')
                email = input('Enter your email address: ')
                contact_book.add_contact(name,phone,email)
            elif choice == '2':
                contact_book.view_contacts()
            elif choice == '3':
                search_name = input('Enter the name to search: ')
                contact_book.search_contact(search_name)
            elif choice == '4':
                delete_name = input('Enter the name to delete the contact: ')
                contact_book.delete_contact(delete_name)
            elif choice == '5':
                print('Exiting the program...')
                break
        except ValueError as e:
            print(f'Error: {e}')

if __name__ == '__main__':
    main()