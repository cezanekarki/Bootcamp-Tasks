from requests import Session

data={
    "title":"BMW Pencil"
}
url = "https://dummyjson.com/products/add"

s1=Session()
response=s1.post(url,data=data)

print(response.text)
print(response.json())
