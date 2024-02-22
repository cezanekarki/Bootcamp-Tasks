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

my_rectangle = Rectangle(5, 7)

print(f'Area: {my_rectangle.calculate_area()}')
print(f'Perimeter: {my_rectangle.calculate_perimeter()}')


def checkIsSquare():
    if(my_rectangle.is_square()):
        print("This rectangle is a square.")
    else:
        print("This rectangle is not a square.")
        
checkIsSquare()
   