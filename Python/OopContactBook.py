import sys
print("Contact Book Application")

class ContactManagement:
    def __init__(self):
        self.contact_book=[
    {'name': 'sabin', 'phone': '9841237438', 'email': 'sabin@gmail.com'},
    {'name': 'kristina', 'phone': '9234567890', 'email': 'kristina@gmail.com'},
    {'name': 'kritik', 'phone': '0987654321', 'email': 'kritk@gmail.com'}
    ]
        self.header = ["Id","Name", "Phone", "Email"]
    @staticmethod   
    def validate_phone_number(phone):
        return phone.isdigit() and len(phone) == 10 and phone.startswith("9")
    
    @staticmethod
    def validate_email(email):
        return '@' in email and '.' in email.split('@')[-1]
    
    def add_contact(self,**contact):
        name=contact['name']
        email=contact['email']
        phone=contact['phone']

        self.contact_book.append({"name": name, "phone": phone, "email": email})
        print("Contact added successfully!")
        self.view_all_contacts()
        
    def view_all_contacts(self):
        print("*****View All Contacts*****")
        print(f"{self.header[0]:<5} {self.header[1]:<15} {self.header[2]:<15} {self.header[3]:<20}")
        
        for index,entry in enumerate(self.contact_book):
            print(f"{index+1:<5} {entry['name']:<15} {entry['phone']:<15} {entry['email']:<20}")

    def search_contact_by_name(self):
        print("*******Search Contact Name*******")
        search_name=input("Enter the contact name: ")
        search_result=self.search_contact_name(search_name)
        if search_result:
            print(f"Contact found:")
            for contact in search_result:
                print(f"Name: {contact['name']}, Phone: {contact['phone']}, Email: {contact['email']}")
        else:
            print(f"Contact Name: '{search_name}' not found.")
        
    def delete_contact_by_name(self):
        print("*****Delete contact name*****")
        search_name=input("Enter the name: ")
        search_result=self.search_contact_name(search_name)
        if search_result:
            self.contact_book = [contact for contact in self.contact_book if contact['name'] != search_name]
            print(f"Contact Name : '{search_result}' deleted successfully.")
            self.view_updated_contact_book()

        else:
            print(f"Contact Name : '{search_name}' not found.")

    
    def search_contact_name(self,name_to_search):
        contacts = list(filter(lambda contact: contact["name"].lower() == name_to_search.lower(), self.contact_book))
        return contacts
        
    def view_updated_contact_book(self):
        print("*******Updated Contact Book*******")
        self.view_all_contacts()
        
def menu_options():
    print("\n********Menu of Options********")
    print("1 - Add Contact")
    print("2 - View Contact")
    print("3 - Search Contact")
    print("4 - Delete Contact")
    print('5 - Exit\n')
  
   
def get_valid_input(prompt, validation_func, error_message):
    while True:
        user_input = input(prompt).strip()
        if validation_func(user_input):
            return user_input
        print(error_message)

def get_user_input():
    name = get_valid_input("Enter your name: ", lambda x: bool(x), "Name is required.")
    phone = get_valid_input("Enter your phone number: ", ContactManagement.validate_phone_number, "Phone number not valid!")
    email = get_valid_input("Enter your email: ", ContactManagement.validate_email, "Email address not valid!")
    return name, phone, email    

def main():
    contact_management_obj=ContactManagement()

    while True:
        menu_options()
        try:
            option=int(input("Enter the option:"))
            if option==1:
                print("*****Add Contact*****")
                name,phone,email=get_user_input()
                contact_management_obj.add_contact(name=name,phone=phone,email=email)
            elif option==2:
                contact_management_obj.view_all_contacts()
            elif option==3:
                contact_management_obj.search_contact_by_name()
            elif option==4:
                contact_management_obj.delete_contact_by_name()
            elif option==5:
                print("You exited from the application")
                sys.exit()
            else:
                print("Wrong Option")
                print("Try selecting available options\n")
            continue
        except ValueError:
            print("Please enter the valid option\n")
            continue
 
if __name__=="__main__":    
     main()
    