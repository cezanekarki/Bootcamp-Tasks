#Initialize List
contacts = []

#Main Meni Function
def menu():
    print("~~~~~ Contact Book Application ~~~~~\n\n* Choose from the provided options *")
    print("1. Add Contact")
    print("2. View All aContacts")
    print("3. Search Contact")
    print("4. Delete Contact")
    print("5. Exit")
    print("\nNote: Please provide the corresponding number of the selected option.")

# Validating phone number
def validate_phone(phone):
    if phone.isdigit() and len(phone) == 10:
        return True
    else:
        print("Invalid phone number! Please enter a 10-digit number.")
        return False

# Validating email
def validate_email(email):
    if '@' in email and '.' in email:
        return True
    else:
        print("Invalid email address! Please enter a valid email.")
        return False

#Contact Adding Function
def add_contact(**kwargs):
    contacts.append(kwargs)
    print("Contact added successfully.\n\nUpdated Contact List:")
    view_contacts()

#Viewing all Contact Function
def view_contacts():
    if not contacts:
        print("\nNo Contacts to be Displayed\n")
    else:
        print("{:<20} {:<15} {:<35}".format("Name", "Phone", "Email")) #Left alighnment with fixed width for each column
        print(*(map(lambda contact: "{:<20} {:<15} {:<35}".format(contact['n'], contact['p'], contact['e']), contacts)), sep='\n') #(*) = Values returned by map sent as seperate arguments.


def search_contact(name):
    if not contacts:
        print("\nNo Contacts to be Displayed\n")
    else:
        filter_contact = lambda contact: contact['n'] == name #Check if name exists in contact
        filtered_contacts = list(filter(filter_contact, contacts))#Storing if the name exists
        if filtered_contacts:
            print("Filtered contacts:")
            print("{:<20} {:<15} {:<35}".format("Name", "Phone", "Email")) #Left alighnment with fixed width for each column
            for contact in filtered_contacts: #Looping through filtered contacts
                print("{:<20} {:<15} {:<35}".format(contact['n'], contact['p'], contact['e']))
        else:
            print("No contacts found with the name", name)


def delete_contact(n):
    global contacts #global because need to delete a contact entirely
    if not contacts:
        print("\nNo Contacts to be Displayed\n")
    else:
        filter_contact = lambda contact: contact['n'] != n #Check if name exists in contact
        filtered_contacts = list(filter(filter_contact, contacts)) #Storing if the name doesnt exists
        if len(filtered_contacts) == len(contacts): #If length is same, name didnt exist in the list
            print(f"\nNo contact found with the name '{n}'.\n")
        else:
            contacts = filtered_contacts #else update the global contacts with filtered contacts which doesnt contain the provided name
            print(f"\nContact '{n}' deleted successfully.\n\nUpdated Contact List:")
            view_contacts()



while True:
    menu()
    choice = input("Enter your choice: ")

    if choice == '1':
        name = input("Enter name: ")
        phone = input("Enter phone: ")
        email = input("Enter email: ")
        if validate_phone(phone) and validate_email(email):
            add_contact(n=name, p=phone, e=email)

    elif choice == '2':
        view_contacts()

    elif choice == '3':
        search_name = input("Enter name to search: ")
        search_contact(search_name)

    elif choice == '4':
        del_name = input("Enter name of contact to delete: ")
        delete_contact(del_name)

    elif choice == '5':
        print("Exiting program.")
        break

    else:
        print("Invalid choice. Please enter a number between 1 and 5.")
    