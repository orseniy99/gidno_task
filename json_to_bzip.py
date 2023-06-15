import bz2
import json
import time


def count_records(filename):
    record_count = 0
    with open(filename, 'r', encoding='utf8') as file:
        for line in file:
            try:
                json_obj = json.loads(line)
                # Count the records based on your data structure
                record_count += 1
            except json.JSONDecodeError:
                pass  # Skip lines that are not valid JSON objects
    return record_count
def convert_to_bz2_utf8_ignore():
    with open("output.json", "r", encoding="utf-8") as f_in, bz2.open("output.ftm.json.bzip", "wt", encoding="utf-8") as f_out:
        for line in f_in:
            f_out.write(line)


if __name__ == '__main__':
    count = count_records('output.json')
    print(f"The JSON file contains {count} records.")
    start_time = time.time()
    convert_to_bz2_utf8_ignore()
    end_time = time.time()
    elapsed_time = end_time - start_time

    # Print the runtime
    print(f"Runtime: {elapsed_time} seconds")