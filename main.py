import json
import os
from dotenv import load_dotenv

import sqlalchemy
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import IntegrityError
from sqlalchemy import select

from models import create_tables, publisher, book, shop, stock, sale

load_dotenv()

# Получение параметров подключения к БД из переменных окружения
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

DB_PORT = int(DB_PORT) if DB_PORT is not None else 5432

# Подключение к БД PostgreSQL
DSN = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = sqlalchemy.create_engine(DSN)

create_tables(engine)

# Создание сессии
Session = sessionmaker(bind=engine)
session = Session()

with open('fixtures/tests_data.json', 'r') as fd:
    data = json.load(fd)

for record in data:
    model = {
        'publisher': publisher,
        'book': book,
        'shop': shop,
        'stock': stock,
        'sale': sale
    }[record.get('model')]

    if record.get('model') == 'sale':
        id_stock = record.get('fields').get('id_stock')
        if session.query(stock).filter_by(id=id_stock).first() is None:
            print(f"Skipping insert for sale record with id_stock={id_stock}")
            continue

    try:
        session.add(model(id=record.get('pk'), **record.get('fields')))
        session.commit()
    except IntegrityError as e:
        session.rollback()
        print(f"IntegrityError: {e}")

# Ввод имени или идентификатора издателя
publisher_name = input("Введите имя или идентификатор издателя: ")

# Запрос фактов покупки книг этого издателя
query = session.query(
    book.title,
    shop.name,
    sale.price,
    sale.date_sale
).join(
    stock,
    stock.id_book == book.id
).join(
    sale,
    sale.id_stock == stock.id
).join(
    publisher,
    publisher.id == book.id_publisher
).join(
    shop,
    shop.id == stock.id_shop
).filter(
    publisher.name == publisher_name or publisher.id == publisher_name
)

# Вывод результатов
for row in query:
    print(f"{row.title} | {row.name} | {row.price} | {row.date_sale}")



session.close()