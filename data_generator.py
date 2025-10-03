import random
import json
import csv
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Any, Union
import string


def generate_random_value(field_type: str) -> Any:
    if field_type == "string":
        return ''.join(random.choices(string.ascii_letters + string.digits, k=random.randint(5, 15)))
    elif field_type == "integer":
        return random.randint(1, 1000)
    elif field_type == "float":
        return round(random.uniform(1.0, 1000.0), 2)
    elif field_type == "boolean":
        return random.choice([True, False])
    elif field_type == "date":
        start_date = datetime(2020, 1, 1)
        end_date = datetime(2024, 12, 31)
        random_date = start_date + timedelta(days=random.randint(0, (end_date - start_date).days))
        return random_date.strftime("%Y-%m-%d")
    elif field_type == "email":
        username = ''.join(random.choices(string.ascii_lowercase + string.digits, k=random.randint(5, 10)))
        domain = random.choice(['gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com', 'example.com'])
        return f"{username}@{domain}"
    elif field_type == "name":
        first_names = ['John', 'Jane', 'Michael', 'Sarah', 'David', 'Emily', 'Robert', 'Lisa', 'James', 'Mary']
        last_names = ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 'Davis', 'Rodriguez', 'Martinez']
        return f"{random.choice(first_names)} {random.choice(last_names)}"
    elif field_type == "age":
        return random.randint(1, 90)
    else:
        return None


def generate_single_record(fields: Dict[str, str]) -> Dict[str, Any]:
    record = {}
    for field_name, field_type in fields.items():
        record[field_name] = generate_random_value(field_type)
    return record


def generate_data(fields: Dict[str, str], num_records: int) -> List[Dict[str, Any]]:
    data = []
    for _ in range(num_records):
        data.append(generate_single_record(fields))
    return data


def generate_json(data: List[Dict[str, Any]], output_file: str = None) -> Union[str, None]:
    json_data = json.dumps(data, indent=2, default=str)
    if output_file:
        with open(output_file, 'w') as f:
            f.write(json_data)
        return None
    return json_data


def generate_csv(data: List[Dict[str, Any]], output_file: str = None) -> Union[str, None]:
    if not data:
        return None
    
    fieldnames = data[0].keys()
    output = []
    
    if output_file:
        with open(output_file, 'w', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(data)
        return None
    else:
        output = []
        output.append(','.join(fieldnames))
        for record in data:
            output.append(','.join(str(record[field]) for field in fieldnames))
        return '\n'.join(output)


def generate_parquet(data: List[Dict[str, Any]], output_file: str) -> None:
    df = pd.DataFrame(data)
    df.to_parquet(output_file, index=False)


def main():
    fields = {
        "id": "integer",
        "name": "name",
        "email": "email",
        "age": "integer",
        "salary": "float",
        "is_active": "boolean",
        "join_date": "date",
        "department": "string"
    }
    
    num_records = 10000
    
    data = generate_data(fields, num_records)
    
    generate_json(data, "output.json")
    generate_csv(data, "output.csv")
    generate_parquet(data, "output.parquet")
    
    print(f"Generated {num_records} records in JSON, CSV, and Parquet formats")


if __name__ == "__main__":
    main()