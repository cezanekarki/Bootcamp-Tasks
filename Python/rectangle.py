class Rectangle:
    length = None
    width = None
    
    def __init__(self, length, width):
        self.length = length
        self.width = width
    
    def calculate_area(self):
        area = self.length * self.width
        return area
    
    def calculate_perimeter(self):
        perimeter = 2 * (self.length + self.width)
        return perimeter
    
    def is_square(self):
        if (self.length == self.width):
            return True
        else:
            return False

my_rectangle = Rectangle(5,7)

print(f'Area: {my_rectangle.calculate_area()}')
print(f'Perimeter: {my_rectangle.calculate_perimeter()}')

if my_rectangle.is_square():
    print('This rectangle is a square.')
else:
    print('This rectangle is not a square.')
        
    
    