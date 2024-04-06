import time
from datetime import datetime
from faker import Faker
import json
import random

fake = Faker()

def generate_fake_data():
    user_id = fake.random_number(digits=4)
    phone_number = fake.numerify(text='##########')

    num_accounts = random.randint(1, 3)  # Randomly choose between 1 to 5 accounts

    user_data_list = []

    # Generate user information
    user_info = {
        "user_id": user_id,
        "name": {
            "first": fake.first_name(),
            "last": fake.last_name() if fake.boolean(chance_of_getting_true=50) else None
        },
        "phone": phone_number
    }

    for _ in range(num_accounts):
        account_number = str(fake.random_number(digits=5)).zfill(6)
        account_data = {
            "account_number": account_number,
            "account_type": fake.random_element(elements=('savings', 'checking', 'investment')),
            "bank_name": fake.random_element(elements=('SBI', 'AXIS', 'HDFC')),
            "balance": fake.random_number(digits=4) + fake.random_number(digits=2) / 100,
            "last_transaction": fake.date_time_this_year(before_now=True, after_now=False, tzinfo=None).isoformat()
        }
        user_data_list.append({"user": user_info, "account": account_data})

    return user_data_list

if __name__ == "__main__":
    curr_time = datetime.now()

    with open("yo_data.json", "w") as json_file:
        while (datetime.now() - curr_time).seconds < 100:
            generated_data = generate_fake_data()

            for user_data in generated_data:
                json_data = json.dumps(user_data)
                json_file.write(json_data + '\n')

            time.sleep(1)

    print("Done.")
