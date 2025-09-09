from pydantic import BaseModel

class User(BaseModel):
    id: int
    name: str
    email: str
    is_active: bool = True  # default value

# Valid data
user = User(id=1, name="Alice", email="alice@example.com")
print(user)  
# {'id': 1, 'name': 'Alice', 'email': 'alice@example.com', 'is_active': True}

# Invalid data (id must be int)
try:
    User(id="abc", name="Bob", email="bob@example.com")
except Exception as e:
    print(e)
