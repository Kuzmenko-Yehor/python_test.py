import requests
import sqlite3
import csv
from datetime import datetime

headers = {'x-fake-auth': 'YmlnIGJsYWNrIGNvY2s'}

response = requests.get('https://raw.githubusercontent.com/lia-diez/fake_json_data/main/fake_data.json', headers=headers)
api_data = response.json()

csv_response = requests.get('https://raw.githubusercontent.com/lia-diez/python-test/refs/heads/main/input/data.csv')
csv_data = csv_response.content.decode('utf-8').splitlines()

def data_phone(phone):
    phone = phone.replace(' ', '').replace('-', '').replace('(', '').replace(')', '')
    if phone.startswith('0'):
        return '+380' + phone[1:]
    elif phone.startswith('+38'):
        return phone
    else:
        return phone

def data_passport(passport, series):
    return f"{series} {passport}" if series else passport

def data_date(date_str, input_format='%d.%m.%Y'):
    try:
        return datetime.strptime(date_str, input_format).strftime(input_format)
    except ValueError:
        return None

conn = sqlite3.connect('citizen_registry.db')
cursor = conn.cursor()

cursor.execute('''
    CREATE TABLE IF NOT EXISTS citizens (
        id INTEGER PRIMARY KEY,
        name TEXT,
        last_name TEXT,
        date_of_birth TEXT,
        sex TEXT,
        phone TEXT,
        email TEXT,
        passport TEXT
    )
''')

for entry in api_data:
    name = entry.get('firstName')
    last_name = entry.get('lastName')
    date_of_birth = data_date(entry.get('dateBorn'), input_format='%d.%m.%Y')
    sex = 'Чоловік' if entry.get('gender') == 'M' else 'Жінка'
    phone = data_phone(entry.get('phoneNumber'))
    email = entry.get('email')
    passport = data_passport(entry['document']['number'], entry['document']['series'])

    cursor.execute('''
        INSERT INTO citizens (name, last_name, date_of_birth, sex, phone, email, passport)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    ''', (name, last_name, date_of_birth, sex, phone, email, passport))

csv_reader = csv.DictReader(csv_data)
for row in csv_reader:
    name = row.get('name')
    last_name = row.get('last_name')
    date_of_birth = data_date(row.get('date_of_birth'), input_format='%Y-%m-%d')
    sex = 'Чоловік' if row.get('sex') == 'True' else 'Жінка'
    phone = data_phone(row.get('phone'))
    email = row.get('email_addr')
    passport = data_passport(row.get('passport'), '')

    cursor.execute('''
        INSERT INTO citizens (name, last_name, date_of_birth, sex, phone, email, passport)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    ''', (name, last_name, date_of_birth, sex, phone, email, passport))

conn.commit()
conn.close()

print("Дані завантажені.")
