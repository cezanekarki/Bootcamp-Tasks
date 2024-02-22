# Databricks notebook source
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


