from pydantic import BaseModel
from typing import Union

class Transaction(BaseModel):
    CustomerID: str
    CompanyName: str
    ContactName: str
    ContactTitle: str
    City: str
    Region: str
    PostalCode: str
    Country: str
    Segment: str
    MetroArea: Union[str, bool]  # 👈 Accepts both
