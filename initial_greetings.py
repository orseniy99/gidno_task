import pandas as pd
import re
import numpy as np

from pandas.api.types import is_string_dtype


# 2920456339020000111
# 2920456339020000111
pd.set_option('display.max_columns', 10)
pd.set_option('display.max_colwidth', None)

initial_dataset = pd.read_csv('contracts_info_initial.csv', sep=';')
# print(initial_dataset['contract_info'])
# print(initial_dataset.dtypes)

initial_dataset['contract_info'] = initial_dataset['contract_info'].astype(str)
# Визначення функції для перевірки чисел і створення нової колонки
def compare_numbers(string):
    pattern = r"{'/contract/([^']+)/': '([^']+)'}"
    match = re.match(pattern, string)

    if match:
        a = match.group(1)
        b = match.group(2)

        return a == b

    return False

# Застосування функції до кожного елемента колонки і створення нової колонки 'check_result'
initial_dataset['check_result'] = initial_dataset['contract_info'].apply(compare_numbers)
# print(initial_dataset[['contract_info', 'check_result']])
counts = initial_dataset['check_result'].value_counts(dropna=False)
print(counts)
print("__________________________________________-")

new_df = initial_dataset.loc[initial_dataset['check_result'] == False].copy()
print(new_df)

# Підрахунок відсутніх значень в кожній колонці
initial_dataset.replace('nan', np.nan, inplace=True)
missing_values = initial_dataset.isna().sum()

# Виведення результатів
print(missing_values)

# Check if there is a row with no empty values
no_empty_values = initial_dataset.notnull().all(axis=1).any()

# Print the result
print(no_empty_values)

# Calculate the number of empty values in each row
empty_values_count = initial_dataset.isna().sum(axis=1)

# Find the minimum number of empty values
min_empty_values = empty_values_count.min()

# Find the indices of the rows with the minimum empty values
rows_with_min_empty_values = empty_values_count[empty_values_count == min_empty_values].index

# Create a new DataFrame with the rows having the minimum empty values
new_df = initial_dataset.loc[rows_with_min_empty_values]

# Print the new DataFrame
# print(new_df.to_string())
#
# print("__________________________________________-")
# print(rows_with_min_empty_values)
