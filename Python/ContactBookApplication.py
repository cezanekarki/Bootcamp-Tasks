
print("Contact Book Application")
contact_book=[
    {'name': 'sabin', 'phone': '9841237438', 'email': 'sabin@gmail.com'},
    {'name': 'kristina', 'phone': '9234567890', 'email': 'kristina@gmail.com'},
    {'name': 'kritik', 'phone': '0987654321', 'email': 'kritk@gmail.com'}
    ]

header = ["Id","Name", "Phone", "Email"]

def validatePhoneNumber(phone):
    return phone.isdigit() and len(phone) == 10 and phone.startswith("9")

def validateEmail(email):
    return '@' in email and '.' in email.split('@')[-1]

def addContact(**contact):
    name=contact['name']
    email=contact['email']
    phone=contact['phone']

    contact = {"name": name, "phone": phone, "email": email}
    contact_book.append(contact)
    print("Contact added successfully!")
    viewAllContacts()
    
def viewAllContacts():
    print("*****View All Contacts*****")
    print(f"{header[0]:<5} {header[1]:<15} {header[2]:<15} {header[3]:<20}")
    
    for index,entry in enumerate(contact_book):
        print(f"{index+1:<5} {entry['name']:<15} {entry['phone']:<15} {entry['email']:<20}")

def searchContactByName():
    print("*******Search Contact Name*******")
    searchName=input("Enter the contact name: ")
    searchResult=searchContactName(searchName)
    if searchResult:
        print(f"Contact found: {searchResult}")
    else:
        print(f"Contact Name : '{searchName}' not found.")
    
def deleteContactByName():
    print("*****Delete contact name*****")
    searchName=input("Enter the name: ")
    searchResult=searchContactName(searchName)
    if searchResult:
        contact_book[:]=filter(lambda contact_name: contact_name['name']!=searchName,contact_book)
    else:
        print(f"Contact Name : '{searchResult}' not found.")
    
def menuOptions():
    print("\n********Menu of Options********")
    print("1 - Add Contact")
    print("2 - View Contact")
    print("3 - Search Contact")
    print("4 - Delete Contact")
    print('5 - Exit\n')
    
def searchContactName(nameToSearch):
    contacts = list(filter(lambda contact: contact["name"] == nameToSearch, contact_book))
    return contacts
    
def viewUpdatedContactBook():
    print("*******Updated Contact Book*******")
    viewAllContacts()
    
def getValidInput(prompt, validation_func, error_message):
    while True:
        user_input = input(prompt).strip()
        if validation_func(user_input):
            return user_input
        print(error_message)

def getUserInput():
    name = getValidInput("Enter your name: ", lambda x: bool(x), "Name is required.")
    phone = getValidInput("Enter your phone number: ", validatePhoneNumber, "Phone number not valid!")
    email = getValidInput("Enter your email: ", validateEmail, "Email address not valid!")
    return name, phone, email

def main():
    while True:
        menuOptions()
        try:
            option=int(input("Enter the option:"))
            if option==1:
                print("*****Add Contact*****")
                name,phone,email=getUserInput()
                addContact(name=name,phone=phone,email=email)
        
            elif option==2:
                viewAllContacts()
            elif option==3:
                searchContactByName()
            elif option==4:
                deleteContactByName()
                viewUpdatedContactBook()
            elif option==5:
                print("You exited from the application")
                break
            else:
                print("Wrong Option")
                print("Try selecting available options\n")
            continue
        except ValueError:
            print("Please enter the valid option\n")
            continue
 
if __name__=="__main__":    
     main()
    