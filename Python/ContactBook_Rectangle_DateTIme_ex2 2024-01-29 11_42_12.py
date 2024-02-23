# Databricks notebook source
# DBTITLE 1,Contact_book

"""def ini_contact_book():
    return []


def add_contact(contact_book, *args, **kwargs):
    contact = {"name": kwargs.get("name", ""),
               "phone": kwargs.get("phone", ""),
               "email": kwargs.get("email", "")}
    contact_book.append(contact)
    print("Contact added successfully!")


def view_all_contacts(contact_book):
    for contact in contact_book:
        print(contact)


def search_contact(contact_book, search_name):
    result = list(filter(lambda x: x["name"].lower() == search_name.lower(), contact_book))
    if result:
        print("Contact found:")
        print(result[0])
    else:
        print("Contact not found.")


def delete_contact(contact_book, delete_name):
    for contact in contact_book:
        if contact["name"].lower() == delete_name.lower():
            contact_book.remove(contact)
            print("Contact deleted successfully!")
            return
    print("Contact not found.")


def main_menu():
    print("\nContact Book Application")
    print("1. Add Contact")
    print("2. View Contacts")
    print("3. Search Contact")
    print("4. Delete Contact")
    print("5. Quit")
    choice = input("Enter your choice (1-5): ")
    return choice


def main():
    contact_book = initialize_contact_book()

    while True:
        choice = main_menu()

        if choice == "1":
            name = input("Enter contact name: ")
            phone = input("Enter contact phone number: ")
            email = input("Enter contact email: ")
            add_contact(contact_book, name=name, phone=phone, email=email)

        elif choice == "2":
            view_all_contacts(contact_book)

        elif choice == "3":
            search_name = input("Enter the name to search: ")
            search_contact(contact_book, search_name)

        elif choice == "4":
            delete_name = input("Enter the name to delete: ")
            delete_contact(contact_book, delete_name)

        elif choice == "5":
            print("Exiting Contact Book Application. Goodbye!")
            break

        else:
            print("Invalid choice. Please enter a number between 1 and 5.")


if __name__ == "__main__":
    main()"""


class ContactBook:
    def __init__(self):
        self.contact_book = []

    def add_contact(self, name, phone, email):
        contact = {"name": name, "phone": phone, "email": email}
        self.contact_book.append(contact)
        print("Contact added successfully!")

    def view_all_contacts(self):
        for contact in self.contact_book:
            print(contact)

    def search_contact(self, search_name):
        result = list(filter(lambda x: x["name"].lower() == search_name.lower(), self.contact_book))
        if result:
            print("Contact found:")
            print(result[0])
        else:
            print("Contact not found.")

    def delete_contact(self, delete_name):
        for contact in self.contact_book:
            if contact["name"].lower() == delete_name.lower():
                self.contact_book.remove(contact)
                print("Contact deleted successfully!")
                return
        print("Contact not found.")

    def main_menu(self):
        print("\nContact Book Application")
        print("1. Add Contact")
        print("2. View Contacts")
        print("3. Search Contact")
        print("4. Delete Contact")
        print("5. Quit")
        choice = input("Enter your choice (1-5): ")
        return choice

    def main(self):
        while True:
            choice = self.main_menu()

            if choice == "1":
                name = input("Enter contact name: ")
                phone = input("Enter contact phone number: ")
                email = input("Enter contact email: ")
                self.add_contact(name, phone, email)

            elif choice == "2":
                self.view_all_contacts()

            elif choice == "3":
                search_name = input("Enter the name to search: ")
                self.search_contact(search_name)

            elif choice == "4":
                delete_name = input("Enter the name to delete: ")
                self.delete_contact(delete_name)

            elif choice == "5":
                print("Exiting Contact Book Application. Goodbye!")
                break

            else:
                print("Invalid choice. Please enter a number between 1 and 5.")


if __name__ == "__main__":
    contact_book_app = ContactBook()
    contact_book_app.main()






# COMMAND ----------


class ContactBook:
    def __init__(self):
        self.contact_book = []

    def add_contact(self, name, phone, email):
        contact = {"name": name, "phone": phone, "email": email}
        self.contact_book.append(contact)
        print("Contact added successfully!")

    def view_all_contacts(self):
        for contact in self.contact_book:
            print(contact)

    def search_contact(self, search_name):
        result = list(filter(lambda x: x["name"].lower() == search_name.lower(), self.contact_book))
        if result:
            print("Contact found:")
            print(result[0])
        else:
            print("Contact not found.")

    def delete_contact(self, delete_name):
        for contact in self.contact_book:
            if contact["name"].lower() == delete_name.lower():
                self.contact_book.remove(contact)
                print("Contact deleted successfully!")
                return
        print("Contact not found.")

    def main_menu(self):
        print("\nContact Book Application")
        print("1. Add Contact")
        print("2. View Contacts")
        print("3. Search Contact")
        print("4. Delete Contact")
        print("5. Quit")
        choice = input("Enter your choice (1-5): ")
        return choice

    def main(self):
        while True:
            choice = self.main_menu()

            if choice == "1":
                name = input("Enter contact name: ")
                phone = input("Enter contact phone number: ")
                email = input("Enter contact email: ")
                self.add_contact(name, phone, email)

            elif choice == "2":
                self.view_all_contacts()

            elif choice == "3":
                search_name = input("Enter the name to search: ")
                self.search_contact(search_name)

            elif choice == "4":
                delete_name = input("Enter the name to delete: ")
                self.delete_contact(delete_name)

            elif choice == "5":
                print("Exiting Contact Book Application. Goodbye!")
                break

            else:
                print("Invalid choice. Please enter a number between 1 and 5.")


if __name__ == "__main__":
    contact_book_app = ContactBook()
    contact_book_app.main()






# COMMAND ----------




class Rectangle:
    def __init__(self, length, width):
        self.length = length
        self.width = width
    
    def calculate_area(self):
        return self.length * self.width
    
    def calculate_perimeter(self):
        return 2 * (self.length + self.width)
    
    def is_sq(self):
        return self.length == self.width

length = int(input("Enter length of Rectangle: "))
width = int(input("Enter width of Rectangle: "))    
my_rect = Rectangle(length, width)

print("area of rectangle is: ",my_rect.calculate_area())
print("perimeter of rectangle is: ",my_rect.calculate_perimeter())

if my_rect.is_sq():
    print("yes")

else:
    print("no")
         







# COMMAND ----------

# MAGIC %sql
# MAGIC from datetime import datetime
# MAGIC
# MAGIC string_json = '{"name": "John Doe", "dob": "1990-01-01 00:00:00", "city": "Anytown", "email": "john.doe@example.com", "is_student": false}'
# MAGIC
# MAGIC dictionary = json.loads(string_json)
# MAGIC
# MAGIC print(dictionary)
# MAGIC
# MAGIC
# MAGIC with open('record.json', 'w') as f:
# MAGIC     json.dump(dictionary, f)
# MAGIC
# MAGIC with open('record.json', 'r') as f:
# MAGIC     read_file = json.load(f)
# MAGIC
# MAGIC read_file['dob'] = datetime.strptime(read_file['dob'], '%Y-%m-%d %H:%M:%S')
# MAGIC
# MAGIC print(read_file)
# MAGIC
# MAGIC
# MAGIC
# MAGIC
