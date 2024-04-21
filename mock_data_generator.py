import pandas as pd
import random
from datetime import datetime, timedelta
import boto3
import os

product_prices = {
    'P12346': 29.99, 'P12347': 39.99, 'P12348': 49.99, 'P12349': 59.99, 'P12350': 69.99, 'P12351': 19.99, 'P12352': 24.99, 'P12353': 29.99, 'P12354': 34.99, 'P12355': 39.99, 'P12356': 39.99, 'P12357': 49.99, 'P12358': 59.99, 'P12359': 69.99, 'P12360': 79.99, 'P12361': 99.99, 'P12362': 149.99, 'P12363': 199.99, 'P12364': 249.99, 'P12365': 299.99
                  }

customer_ids = [
    'C12346', 'C12347', 'C12348', 'C12349', 'C12350', 'C12351', 'C12352', 'C12353', 'C12354', 'C12355', 'C12356', 'C12357', 'C12358', 'C12359', 'C12360', 'C12361', 'C12362', 'C12363', 'C12364', 'C12365'
    ]


def generate_unique_transaction_id(transac_used_ids):
    while True:
        transaction_id = "TXN" + str(random.randint(1, 1000)).zfill(4)
        if transaction_id not in transac_used_ids:
            transac_used_ids.add(transaction_id)
            return transaction_id

transac_used_ids = set()
records = []

#generate the records 
def generate_records():    
    for _ in range(100):
        transaction_id = generate_unique_transaction_id(transac_used_ids)
        customer_id = f'{random.choice(customer_ids)}'
        product_id_price_num = random.randint(0,len(product_prices)-1)
        product_id, price = list(product_prices.items())[product_id_price_num]
        quantity = random.randint(1, 10)
        payment_type = f'{random.choice(("Credit Card", "Debit Card", "UPI"))}'
        status = 'Completed'
        transaction_date = datetime.now().date()
        records.append((transaction_id, customer_id, product_id, quantity, price, transaction_date, payment_type, status))
    print('All Records Generated Sucessfully !')

generate_records()
curr_date = datetime.now().date() #+ timedelta(days=1)
#saving records into the dataframe 
df = pd.DataFrame(records, columns=["transaction_id", "customer_id", "product_id", "quantity", "price", "transaction_date", "payment_type", "status"])

# 
# #writing the records into csv



# # Create directory if it doesn't exist
directory = f'./my_bucket/transactions/year={curr_date.year}/month={curr_date.month}/day={curr_date.day}/'
os.makedirs(directory, exist_ok=True)

#Save the DataFrame to CSV
df.to_csv(os.path.join(directory, f'transactions{curr_date}.csv'), index=False)

print('uploaded in local')

s3_client = boto3.client('s3')
bucket_name = 'ass-ecommerce-mod3'
file_name = f'transaction{curr_date}.csv'
file_path = f"transactions/year={curr_date.year}/month={curr_date.month}/day={curr_date.day}/{file_name}"

s3_client.upload_file(f'./my_bucket/transactions/year={curr_date.year}/month={curr_date.month}/day={curr_date.day}/transactions{curr_date}.csv', bucket_name, file_path)

print('Uploaded to S3')

# df.to_csv(f'./my_bucket/transactions/year={curr_date.year}/month={curr_date.month}/day={curr_date.day}/transactions{curr_date}.csv', index=False)