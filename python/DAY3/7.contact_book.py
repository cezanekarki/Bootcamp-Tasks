class ContactBook:
    def __init__(self):
        self.contacts = []

    def add_contact(self, name, phone, email):
        contact = {"name": name, "phone": phone, "email": email}
        self.contacts.append(contact)

    def view_all_contacts(self):
        if self.contacts:
            for index, contact in enumerate(self.contacts, start=1):
                print(f"{index}. {contact}")
        else:
            print("Contact book is empty.")

    def search_contact(self, name):
        name_lower = name.lower()
        found_contacts = list(filter(lambda x: x['name'].lower() == name_lower, self.contacts))
        if found_contacts:
            for contact in found_contacts:
                print(contact)
        else:
            print("Contact not found.")

    def delete_contact(self, name):
        name_lower = name.lower()
        initial_length = len(self.contacts)
        self.contacts = [contact for contact in self.contacts if contact['name'].lower() != name_lower]
        return len(self.contacts) < initial_length

    def contact_book(self):
        while True:
            print("\nContact Book Menu:")
            print("1. Add Contact")
            print("2. View All Contacts")
            print("3. Search Contact")
            print("4. Delete Contact")
            print("5. Quit")

            choice = input("Enter your choice: ")

            if choice == "1":
                name = input("Enter name: ")
                phone = input("Enter phone number: ")
                email = input("Enter email: ")
                self.add_contact(name, phone, email)
                print("Contact added successfully.")

            elif choice == "2":
                self.view_all_contacts()

            elif choice == "3":
                name = input("Enter name to search: ")
                self.search_contact(name)

            elif choice == "4":
                name = input("Enter name to delete: ")
                if self.delete_contact(name):
                    print("Contact deleted successfully.")
                else:
                    print("No contact found with that name.")

            elif choice == "5":
                print("Exiting Contact Book. Goodbye!")
                break

            else:
                print("Invalid choice. Please try again.")


def main():
    contact_book = ContactBook()
    contact_book.contact_book()


if __name__ == "__main__":
    main()

