class ContactBook:
    def __init__(self):
        self.contacts = []

    def add_contact(self, name, phone, email):
        user_dict = {
            "name": name,
            "phone": phone,
            "email": email
        }
        self.contacts.append(user_dict)
        print(f"You have added {name} successfully.")
        print(f"The updated contact book is: {self.contacts}")

    def search_contact(self):
        name = input("Which contact do you want to retrieve?")
        found = False

        search_lambda = lambda x: x["name"].lower() == name.lower()
        found_contacts = list(filter(search_lambda, self.contacts))

        if found_contacts:
            for contact in found_contacts:
                print(f"For {contact['name']}, the phone number is {contact['phone']} and email is {contact['email']}")
            found = True
        else:
            print(f"Contact {name} not found in the contact book.")

    def view_contact(self):
        if not self.contacts:
            print("Contact book is empty.")
        else:
            for contact in self.contacts:
                print(f"Contact: {contact}")

    def delete_contact(self):
        name = input("Which contact do you want to delete?")

        if self.contacts:
            self.contacts = [contact for contact in self.contacts if contact["name"] != name]
            print(f"Contact {name} deleted successfully!")
        else:
            print(f"The {name} not available.")
        print(f"The updated contact book is: {self.contacts}")


class Menu:
    def __init__(self):
        self.contact_book = ContactBook()

    def start(self):
        while True:
            print("\n<<<<<< Welcome to the world of contact book >>>>>>")
            print("1. Add Contact")
            print("2. View Contact")
            print("3. Search Contact")
            print("4. Delete Contact")
            print("5. Quit")

            chosen = input("Choose any number from the above menu.... ")

            if chosen == "1":
                name = input("Enter the name of the contact:")
                phone = input("Enter the phone number of the contact:")
                email = input("Enter the email of the contact:")
                self.contact_book.add_contact(name, phone, email)
            elif chosen == "2":
                self.contact_book.view_contact()
            elif chosen == "3":
                self.contact_book.search_contact()
            elif chosen == "4":
                self.contact_book.delete_contact()
            elif chosen == "5":
                print("Thank you. See you again")
                break
            else:
                print("Please choose a number between 1 and 5.")


if __name__ == "__main__":
    menu = Menu()
    menu.start()
