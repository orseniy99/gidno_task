from followthemoney import model
from followthemoney.namespace import Namespace
from followthemoney.types import registry
import pandas as pd
import numpy as np
import json
import bz2
import ast
import time
def clean_phone_numbers(phone_numbers_str):
    # Parse the string into a list
    phone_numbers = ast.literal_eval(phone_numbers_str)

    cleaned_numbers = []
    for number in phone_numbers:
        cleaned_number = number.replace(" ", "").replace("-", "")
        cleaned_numbers.append(cleaned_number)
    return cleaned_numbers


def get_nonempty_field(dictionary):
    fields = ['item', 'items', 'service', 'services']
    for field in fields:
        if dictionary.get(field) is not None:
            return dictionary.get(field)
    return None

def parse_list_string(list_string):
    # Remove leading/trailing whitespaces and square brackets
    list_string = list_string.strip("[]")
    if not list_string:
        return None
    # Parse the string into a list
    parsed_list = ast.literal_eval(list_string)
    return parsed_list
def iterate_dataframe_rows(df):
    # Iterate through each row of the DataFrame
    for index, row in df.iterrows():
        # Convert the row to a dictionary
        row_dict = row.to_dict()

        # Yield the row as a dictionary
        yield row_dict

def generate_json_line(row):
    # Proxy entities generation
    contract_entity = model.make_entity(model.get('Contract'))
    customer_entity = model.make_entity(model.get('Company'))
    supplier_entity = model.make_entity(model.get('Company'))
    contract_ownership_entity = model.make_entity(model.get('Ownership'))

    # initialization of proxy
    contract_entity.make_id(row.get('contract_ID'), row.get('contract_sum'))
    customer_entity.make_id(row.get('customer_inn'), row.get('customer_kpp'))
    supplier_entity.make_id(row.get('supplier_inn'), row.get('supplier_kpp'))
    contract_ownership_entity.make_id(row.get('contract_ID'))

    # Contract Ownership (Customer)
    contract_ownership_entity.add('asset', contract_entity)
    contract_ownership_entity.add('owner', customer_entity)

    # Customer EntityProxy
    customer_entity.add('name', row.get('customer_name'))
    customer_entity.add('innCode', row.get('customer_inn'))
    customer_entity.add('kppCode', row.get('customer_kpp'))
    customer_entity.add('notes', str(row.get('customer_crc'))+'_crc')
    customer_entity.add('phone', clean_phone_numbers(row.get('customer_phones_list')))
    customer_entity.add('email', parse_list_string(row.get('customer_emails_list')))

    # Contract ProxyEntity
    contract_entity.add('title', row.get('contract_ID'))
    contract_entity.add('contractDate', row.get('contract_sign_date'))
    contract_entity.add('amount', row.get('contract_sum'))
    contract_entity.add('description', get_nonempty_field(row))
    contract_entity.add('indexText', row.get('guid_contract'))
    contract_entity.add('sourceUrl', row.get('contract_url'))
    contract_entity.add('authority', supplier_entity)

    # Supplier EntityProxy
    supplier_entity.add('name', row.get('supplier_name'))
    supplier_entity.add('innCode', row.get('supplier_inn'))
    supplier_entity.add('kppCode', row.get('supplier_kpp'))
    supplier_entity.add('amount', row.get('supplier_num_contracts'))
    supplier_entity.add('amount', row.get('supplier_sum_contracts'))
    supplier_entity.add('phone', clean_phone_numbers(row.get('supplier_phones_list')))
    supplier_entity.add('email', parse_list_string(row.get('supplier_emails_list')))


    customer_data = customer_entity.to_dict()
    ownership_data = contract_ownership_entity.to_dict()
    supplier_data = supplier_entity.to_dict()
    contract_data = contract_entity.to_dict()

    json_line = [
        customer_data,
        ownership_data,
        supplier_data,
        contract_data,
    ]

    # print(row)
    return json_line

# Запис буфера до JSON файлу
def write_buffer_to_json(buffer):
    with open("output.json", "a", encoding="utf-8") as f:
        for entity in buffer:
            f.write(json.dumps(entity, ensure_ascii=False))
            f.write('\n')
def iter_transform(generator):
    buffer = []
    now_recorded = 0
    for row_dict in generator:
        line_data = generate_json_line(row_dict)
        buffer.extend(line_data)

        # Запис буфера в JSON файл після заповнення
        if len(buffer) >= buffer_size:
            now_recorded += len(buffer)
            print(f"Now recorded: {now_recorded}")
            write_buffer_to_json(buffer)
            buffer = []

    # Запис залишків буфера у JSON файл
    if buffer:
        write_buffer_to_json(buffer)

def convert_to_bz2():
    with open("output.json", "r", encoding="utf-8") as f_in, bz2.open("output.ftm.json.bzip", "wt") as f_out:
        for line in f_in:
            f_out.write(line)

if __name__ == '__main__':
    buffer_size = 1000
    raw_data = pd.read_csv('cleaned_data.csv', sep=";")
    raw_data = raw_data.replace(np.nan, None)
    raw_data = raw_data.drop(columns=raw_data.columns[0])
    df_generator = iterate_dataframe_rows(raw_data)
    iter_transform(df_generator) ## Створює json
    print('Sleep before zipping')
    time.sleep(10)
    convert_to_bz2()
