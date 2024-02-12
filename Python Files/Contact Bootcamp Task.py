contacts = []

def add_contact(*args, **kwargs):
    name = input("Enter name: ")
    phone = input("Enter phone number: ")
    email = input("Enter email: ")

    contact = {"name": name, "phone": phone, "email": email}
    contacts.append(contact)
    print("Contact added successfully!")

def view_all_contacts():
    print("All Contacts:")
    for contact in contacts:
        print(f"Name: {contact['name']}, Phone: {contact['phone']}, Email: {contact['email']}")
    print()

def search_contact_by_name(*args, **kwargs):
    name = input("Enter name to search: ")
    result = list(filter(lambda x: x["name"].lower() == name.lower(), contacts))
    if result:
        print("Search Results:")
        for contact in result:
            print(f"Name: {contact['name']}, Phone: {contact['phone']}, Email: {contact['email']}")
    else:
        print("No matching contact found.")

def delete_contact_by_name(*args, **kwargs):
    name = input("Enter name to delete: ")
    global contacts
    contacts = [contact for contact in contacts if contact["name"].lower() != name.lower()]
    print(f"Contact '{name}' deleted successfully!")

def main_menu():
    while True:
        print("Contact Book Application")
        print("1. Add Contact")
        print("2. View All Contacts")
        print("3. Search Contact")
        print("4. Delete Contact")
        print("5. Quit")

        choice = input("Enter your choice (1-5): ")

        if choice == "1":
            add_contact()
        elif choice == "2":
            view_all_contacts()
        elif choice == "3":
            search_contact_by_name()
        elif choice == "4":
            delete_contact_by_name()
        elif choice == "5":
            print("Exiting Contact Book. Goodbye!")
            break
        else:
            print("Invalid choice. Please enter a number between 1 and 5.")

if __name__ == "__main__":
    main_menu()
