# 4. You have a dictionary of stock quantities for various items and a list of items that have been sold. Update the dictionary by reducing the stock quantity for each sold item. 
 
# stock = {"apples": 10, "oranges": 8, "bananas": 6} 
# sold_items = ["apples", "oranges", "apples", "bananas"]

# SOLUTION
stock = {"apples": 10, "oranges": 8, "bananas": 6} 
sold_items = ["apples", "oranges", "apples", "bananas"]

for i in sold_items:
    if i in stock.keys():
        stock[i] -= 1
    else:
        print("Item not available")

print(f"Remaining items: {stock}")