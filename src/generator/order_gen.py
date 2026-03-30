from faker import Faker
import random
from bson import ObjectId

fake = Faker()

def generate_order():
    return {
        "_id": ObjectId(),
        "customer_id": fake.uuid4(),
        "status": "PENDING",
        "city": fake.city(),
        "total": round(random.uniform(10.0, 500.0), 2)
    }

