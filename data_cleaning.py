import pandas as pd
import re
import numpy as np

pd.set_option('display.max_colwidth', None)

initial_dataset = pd.read_csv('contracts_info_initial.csv', sep=';')
initial_dataset = initial_dataset.drop(initial_dataset.columns[[0, 1]], axis=1)
initial_dataset = initial_dataset.drop(['contract_num'], axis=1)

initial_dataset = initial_dataset.astype(str)



# Function to extract the value from the contract_url
def extract_value(url):
    start_index = url.find('/contract/') + len('/contract/')
    end_index = url.find('/', start_index)
    return url[start_index:end_index]

initial_dataset['contract_ID'] = initial_dataset['contract_info'].apply(extract_value)
initial_dataset = pd.concat([initial_dataset['contract_ID'], initial_dataset.drop('contract_ID', axis=1)], axis=1)

def get_customer_name(string):
    match = re.search(r": '([^']+)'", string)
    if match:
        result = match.group(1)
        return result.upper()

initial_dataset['customer_name'] = initial_dataset['customer_info'].apply(get_customer_name)
initial_dataset = initial_dataset.drop(['contract_info', 'customer_info', 'customer_url'], axis=1)

def get_supplier_kpp(string):
    match = re.search(r"kpp=(\d+).", string)
    if match:
        result = match.group(1)
        return result

initial_dataset['supplier_kpp'] = initial_dataset['supplier_url'].apply(get_supplier_kpp)
initial_dataset = initial_dataset.drop(['supplier_url'], axis=1)

custom_order = [
    'contract_ID',
    'contract_sign_date',
    'contract_end_date',
    'contract_sum',
    'item',
    'items',
    'service',
    'services',
    'guid_contract',
    'contract_url',
    'supplier_inn',
    'supplier_kpp',
    'supplier_name',
    'supplier_num_contracts',
    'supplier_sum_contracts',
    'supplier_phones_list',
    'supplier_emails_list',
    'customer_inn',
    'customer_kpp',
    'customer_crc',
    'customer_name',
    'customer_phones_list',
    'customer_emails_list',
]

initial_dataset = initial_dataset.reindex(columns=custom_order)
initial_dataset.to_csv('cleaned_data.csv', sep=";")
print(None)

# 2312301229822000215
# 2312301229822000215