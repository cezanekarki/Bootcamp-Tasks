'''
Create a Python class named Rectangle to represent rectangles. The class should have the following attributes and methods:

Attributes:

length (positive float): Length of the rectangle.

width (positive float): Width of the rectangle.

Methods:

__init__(self, length, width): Constructor method to initialize the length and width attributes.
calculate_area(self): Method that calculates and returns the area of the rectangle.
calculate_perimeter(self): Method that calculates and returns the perimeter of the rectangle.
is_square(self): Method that returns True if the rectangle is a square (length equals width), otherwise returns False.
Write a program that creates an instance of the Rectangle class, sets the length and width, and then prints the area, perimeter, and whether the rectangle is a square.
'''

class Rectangle:
    def __init__(self, length, width):
        self.length = length
        self.width = width

    def calculate_area(self):
        return self.length * self.width

    def calculate_perimeter(self):
        return 2 * (self.length + self.width)

    def is_square(self):
        return self.length == self.width
    
    def __str__(self):
        return f'Length: {self.length}, Width: {self.width}, Area: {self.calculate_area()}, Perimeter: {self.calculate_perimeter()}, Is Square: {self.is_square()}'
    
    
    
if __name__ == '__main__':
    rectangle = Rectangle(10, 10)
    print(rectangle)
    rectangle = Rectangle(10, 20)
    print(rectangle)
    rectangle = Rectangle(20, 10)
    print(rectangle)
    rectangle = Rectangle(20, 20)
    print(rectangle)
    rectangle = Rectangle(10, 30)
    print(rectangle)
