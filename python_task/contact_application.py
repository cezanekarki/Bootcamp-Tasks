
# Initialize the contact book as an empty list
contact_book = []

# Function to add a contact
def add_contact(name, phone, email):
    contact = {"name": name, "phone": phone, "email": email}
    contact_book.append(contact)
    print(f"Contact {name} added successfully.")

# Function to view all contacts
def view_contacts():
    for contact in contact_book:
        print(contact)

# Function to search for a contact by name
search_contact = lambda name: [contact for contact in contact_book if contact["name"].lower() == name.lower()]

def delete_contact(name):
    global contact_book
    contacts_before_deletion = len(contact_book)
    
    contact_book = [contact for contact in contact_book if contact["name"].lower() != name.lower()]
    
    if len(contact_book) == contacts_before_deletion:
        print(f"Contact {name} not found in the contact book. Deletion aborted.")
    else:
        print(f"Contact {name} deleted successfully.")

# Function to validate phone number format
def validate_phone(phone):
    return phone.isdigit() and len(phone) == 10 and phone.startswith('9')


import re 
def validate_email(email):
    regex = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,7}\b'
    if(re.fullmatch(regex,email)):
        return True
    else:
        return False


# Main menu
while True:
    print("\nContact Book Menu:")
    print("1. Add Contact")
    print("2. View Contacts")
    print("3. Search Contact")
    print("4. Delete Contact")
    print("5. Quit")

    choice = input("Enter your choice (1-5): ")

    if choice == "1":
        name = input("Enter the name: ")
        phone = input("Enter the phone number: ")
        email = input("Enter the email address:")

        while not validate_phone(phone):
            print("Invalid phone number format. Please enter a 10-digit number starting with 9.")
            phone = input("Enter the phone number: ")

        
        
        while not  validate_email(email):
            print(f"The email is invalid please enter the valid email")
            email = input("Enter the email (optional): ")
            
        add_contact(name, phone, email)
        input('Press any key...')

    elif choice == "2":
        if not contact_book:
            print("Contact book is empty.")
        else:
            print("\nAll Contacts:")
            view_contacts()
        input('Press any key...')

    elif choice == "3":
        name_to_search = input("Enter the name to search: ")
        search_result = search_contact(name_to_search)

        if not search_result:
            print(f"No contacts found with the name {name_to_search}.")
        else:
            print(f"\nSearch Result for {name_to_search}:")
            view_contacts()
        input('Press any key...')

    elif choice == "4":
        name_to_delete = input("Enter the name to delete: ")
        delete_contact(name_to_delete)
        input('Press any key...')

    elif choice == "5":
        print("Exiting Contact Book. Goodbye!")
        break

    else:
        print("Invalid choice. Please enter a number between 1 and 5.")
