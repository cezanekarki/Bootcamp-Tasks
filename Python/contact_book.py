contact_list = []
contact= {}

def each_contact(search_name):
    for items in contact_list:
        if items.get(search_name):
            return items.get(search_name)

def check_valid_email(my_email):
    email_list = []
    for items in my_email:
        email_list.append(items)

    if '@' in email_list:
        if '.' in email_list:
            return True
    else:
        return False    

def length_checker(phone_str):
    if len(phone_str) != 10:
        raise Exception()

            



while True:
    option = int(input("1. Add Contact \n2. View all contact \n3. Search for a contact by name \n4. Delete a contact \n5. Exit \n"))

    if option == 1:
        name = input("Enter contact name ")   
        try: 
            phone = int(input("Enter phone number "))
            phone_str = str(phone)
            length_checker(phone_str)
        
        except:
            print("Check check you phone number")
            continue
        address = input("Enter address ")

        email = input("Enter contact email ")
        if check_valid_email(email) == True:
            contact = {
                name : {
                    'name' : name,
                    'phone' : phone,
                    'address' : address,
                    'email' : email
                }
        }
            contact_list.append(contact)
            print("Contact Added successfully")
        else:
            print("Please enter a valid email address")

        

    if option == 2:
        print(contact_list)

    if option == 3:
        search_name = input("Enter the name for search ")
        contact1 = each_contact(search_name)
        if search_name == contact1['name']:
            print(f"The contact info is {contact1}")

        else:
            print("name not found")

    if option == 4:
            delete_contact = input("Enter the name of person to delete the contact detail")
            for items in contact_list:
                if items.get(delete_contact):
                    items.pop(delete_contact)
                else:
                    print("Not Found")    
                

    if option == 5:
        break
            




