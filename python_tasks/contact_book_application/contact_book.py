
contacts = []
def add_contact(*args):
    contact = {}
    
    for arg in args:
        contact[arg] = input(f"Enter {arg}: ")
    
    contacts.append(contact)
    print("Contact added successfully!")

def view_contacts():
    if not contacts:
        print("No contacts found.")
    else:
        for i, contact in enumerate(contacts, 1):
            print(f"{i}. {contact}")

def search_contact_by_name(name):
    result = list(filter(lambda x: x.get("name") == name, contacts))
    if result:
        print("Contact found:")
        print(result)
    else:
        print("Contact not found.")

def delete_contact_by_name(name):
    global contacts
    contacts = list(filter(lambda x: x.get("name") != name, contacts))
    print("Contact is deleted successfully!")

def main_menu():
    while True:
        print("\n----- Contact Book Menu -----")
        print("1. Add Contact")
        print("2. View Contacts")
        print("3. Search Contact")
        print("4. Delete Contact")
        print("5. Quit")
        
        choice = input("Enter your choice (1-5): ")
        
        if choice == "1":
            add_contact("name", "phone", "email")
        elif choice == "2":
            view_contacts()
        elif choice == "3":
            name = input("Enter name to search: ")
            search_contact_by_name(name)
        elif choice == "4":
            name = input("Enter name to delete: ")
            delete_contact_by_name(name)
        elif choice == "5":
            print("Exiting Contact Book")
            break
        else:
            print("Invalid choice. Please enter a number between 1 and 5.")

main_menu()
