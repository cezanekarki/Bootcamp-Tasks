import re

# Initialize the contact book as an empty list
contact_book = []

# Function to add a contact
def add_contact(name, phone, email):
    contact = {"name": name, "phone": phone, "email": email}
    contact_book.append(contact)
    print(f"Contact {name} added successfully.")

# Function to view all contacts
def view_contacts():
    if not contact_book:
        print("Contact book is empty.")
    else:
        for index, contact in enumerate(contact_book, start=1):
            print(f"Contact {index}:")
            print(f"Name: {contact['name']}")
            print(f"Phone: {contact['phone']}")
            print(f"Email: {contact['email']}")
            print()

# Function to search for a contact by name
def search_contact(name):
    return [contact for contact in contact_book if contact["name"].lower() == name.lower()]

# Function to delete a contact by name
def delete_contact(name):
    global contact_book
    initial_contact_count = len(contact_book)
    contact_book = [contact for contact in contact_book if contact["name"].lower() != name.lower()]

    if len(contact_book) == initial_contact_count:
        print(f"Contact {name} not found in the contact book. Deletion aborted.")
    else:
        print(f"Contact {name} deleted successfully.")

# Function to validate phone number format
def validate_phone(phone):
    return phone.isdigit() and len(phone) == 10 and phone.startswith('9')

# Function to validate email format
def validate_email(email):
    regex = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,7}\b'
    return bool(re.fullmatch(regex, email))

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
        email = input("Enter the email address (optional): ")

        while not validate_phone(phone):
            print("Invalid phone number format. Please enter a 10-digit number starting with 9.")
            phone = input("Enter the phone number: ")

        while email and not validate_email(email):
            print("Invalid email address format.")
            email = input("Enter the email address (optional): ")

        add_contact(name, phone, email)

    elif choice == "2":
        view_contacts()

    elif choice == "3":
        name_to_search = input("Enter the name to search: ")
        search_result = search_contact(name_to_search)

        if not search_result:
            print(f"No contacts found with the name {name_to_search}.")
        else:
            print(f"\nSearch Result for {name_to_search}:")
            for contact in search_result:
                print(f"Name: {contact['name']}")
                print(f"Phone: {contact['phone']}")
                print(f"Email: {contact['email']}")
                print()

    elif choice == "4":
        name_to_delete = input("Enter the name to delete: ")
        delete_contact(name_to_delete)

    elif choice == "5":
        print("Exiting Contact Book. Goodbye!")
        break

    else:
        print("Invalid choice. Please enter a number between 1 and 5.")
