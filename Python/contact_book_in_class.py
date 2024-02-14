class Contact:
    contact_book = []
    
    def add_contact(self, **contact):
        self.contact_book.append({'name': contact['name'], 'phone_number': contact['phone_number'], 'email': contact['email']})

    def user_input_to_add_contact(self):
        name = input('Enter name: ')
        phone_number = input('Enter phone number: ')
        email = input('Enter email: ')
        self.add_contact(name=name, phone_number=phone_number, email=email)
    
    def view_contacts(self):
        if self.contact_book:
            print('List of all contacts: ', self.contact_book)
        else:
            print('No contact available')

    def search_contact(self):
        name = input('Search contact by name: ')
        searched_contact = list(filter(lambda contact: contact['name'] == name ,self.contact_book))
        if searched_contact:
            print('\nContact of {} is :\n{} '.format(name, *searched_contact))
        else:
            print('\nNo contact available')

    def delete_contact(self):
        name = input('Enter name of contact to delete: ')
        for contact in self.contact_book:
            if contact.get('name') == name:
                self.contact_book.remove(contact)
                break
        else:
            print('There is no contact for ', name)



print('-----------------Contact Book Application-----------------')

contact1 = Contact()
while True:
    user_action = int(input('\nEnter\n 1 to add contact\n 2 to view contact\n 3 to search contact\n 4 to delete contact\n 5 to quit the program\n'))
    if(user_action == 5):
        print('Program exited')
        break
    elif(user_action == 1):
        contact1.user_input_to_add_contact()
    elif(user_action == 2):
        contact1.view_contacts()
    elif(user_action == 3):
        contact1.search_contact()
    elif(user_action == 4):
        contact1.delete_contact()
    print('\nUpdated contact :', contact1.contact_book)
