class Rectangle:
    def __init__(self, length, width):
        self.length = length
        self.width = width

    def calculate_area(self):
        return self.length * self.width

    def calculate_perimeter(self):
        return 2 * (self.length + self.width)

    def is_square(self):
        if self.length == self.width:
            return True
        else:
            return False

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
