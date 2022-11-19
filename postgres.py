import psycopg2
from psycopg2 import Error

try:
    # Подключиться к существующей базе данных
    connection = psycopg2.connect(user="postgres",
                                  # пароль, который указали при установке PostgreSQL
                                  password="",
                                  host="127.0.0.1",
                                  port="5432",
                                  database="template1")

    # Создайте курсор для выполнения операций с базой данных
    cursor = connection.cursor()
    # SQL-запрос для создания новой таблицы
    create_table_query = '''CREATE TABLE advertisements
                          (ID INT PRIMARY KEY     NOT NULL,
                          URL         TEXT,
                          PRICE       REAL,
                          CURRENCY    TEXT,
                          DATE_POSTED DATE); '''
    # Выполнение команды: это создает новую таблицу
    cursor.execute(create_table_query)
    connection.commit()
    print("Таблица успешно создана в PostgreSQL")

except (Exception, Error) as error:
    print("Ошибка при работе с PostgreSQL", error)
finally:
    if connection:
        cursor.close()
        connection.close()
        print("Соединение с PostgreSQL закрыто")
