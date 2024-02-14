contact_book = []

def add_contact(**contact):
    contact_book.append({'name': contact['name'], 'phone_number': contact['phone_number'], 'email': contact['email']})

def user_input_to_add_contact():
    name = input('Enter name: ')
    phone_number = input('\nEnter phone number: ')
    email = input('\nEnter email: ')
    add_contact(name=name, phone_number=phone_number, email=email)
    
def view_contacts():
    if len(contact_book) > 0:
        print('List of all contacts: ', contact_book)
    else:
        print('No contact available')

def search_contact():
    name = input('Search contact by name: ')
    searched_contact = list(filter(lambda contact: contact['name'] == name ,contact_book))
    if len(searched_contact) > 0:
        print('\nContact of {} is :\n{} '.format(name, *searched_contact))
    else:
        print('\nNo contact available')

def delete_contact():
    name = input('Enter name of contact to delete: ')
    for contact in contact_book:
        if contact.get('name') == name:
            contact_book.remove(contact)
            break
    else:
        print('There is no contact for ', name)


print('-----------------Contact Book Application-----------------')

while True:
    user_action = int(input('\nEnter\n 1 to add contact\n 2 to view contact\n 3 to search contact\n 4 to delete contact\n 5 to quit the program\n'))
    if(user_action == 5):
        print('Program exited')
        break
    elif(user_action == 1):
        user_input_to_add_contact()
    elif(user_action == 2):
        view_contacts()
    elif(user_action == 3):
        search_contact()
    elif(user_action == 4):
        delete_contact()
    print('\nUpdated contact :', contact_book)
