"""

Create a Python class named Rectangle to represent rectangles. The class should have the following attributes and methods:

Attributes:

* length (positive float): Length of the rectangle.

* width (positive float): Width of the rectangle.


Methods:

* \_\_init\_\_(self, length, width): Constructor method to initialize the length and width attributes.
* calculate_area(self): Method that calculates and returns the area of the rectangle.
* calculate_perimeter(self): Method that calculates and returns the perimeter of the rectangle.
* is_square(self): Method that returns True if the rectangle is a square (length equals width), otherwise returns False.

Write a program that creates an instance of the Rectangle class, sets the length and width, and then prints the area, perimeter, and whether the rectangle is a square.

"""


class Rectangle:
    def __init__(self,length,width):
        self.length = length
        self.width = width

    def calculate_area(self):
        return self.length*self.width

    def calculate_perimeter(self):
        return 2(self.length+self.width)

    def is_square(self):
        if self.length == self.width:
            return True
        else:
            False

# Create a rectangle with length 5 and width 7
my_rectangle = Rectangle(5, 7)

# Print area and perimeter
print(f"Area: {my_rectangle.calculate_area()}")
print(f"Perimeter: {my_rectangle.calculate_perimeter()}")

# Check if the rectangle is a square
if my_rectangle.is_square():
    print("This rectangle is a square.")
else:
    print("This rectangle is not a square.")
