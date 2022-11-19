import requests
from bs4 import BeautifulSoup
from datetime import date, datetime
import psycopg2

try:
    # Подключиться к существующей базе данных
    connection = psycopg2.connect(user="postgres",
                                  # пароль, который указали при установке PostgreSQL
                                  password="",
                                  host="127.0.0.1",
                                  port="5432",
                                  database="template1")
    
    pk = 1
    for page in range(1,100):
        url = 'https://www.kijiji.ca/b-apartments-condos/city-of-toronto/page-'+ str(page) +'/c37l1700273'
        r = requests.get(url)
        soup = BeautifulSoup(r.text, 'html.parser')
        res = soup.findAll('div', class_='clearfix')
        for i in range(1, 46):
            image = res[i].find('img').get('data-src')
            if image is None:
                image = 'no image'
            price = res[i].find('div', class_='price').text
            price = price.strip()
            if price == 'Please Contact':
                currency = '$'
                price = 0.0
            else:
                currency = price[0]
                price = float(price[1:-1].replace(',',''))
            date_posted = res[i].find('span', class_='date-posted').text
            if date_posted[:-9 - 1:-1] =='oga sruoh':
                date_posted = date.today()
            else:
                date_posted = datetime.strptime(date_posted, '%d/%m/%Y').date()
            cursor = connection.cursor()
            # Выполнение SQL-запроса для вставки даты и времени в таблицу
            insert_query = """ INSERT INTO advertisements (ID, URL, PRICE, CURRENCY, DATE_POSTED)
                                  VALUES (%s, %s, %s, %s, %s)"""
            item_tuple = (pk, image, price, currency, date_posted)
            cursor.execute(insert_query, item_tuple)
            connection.commit()
            print("1 элемент успешно добавлен")
            pk +=1

except (Exception, Error) as error:
    print("Ошибка при работе с PostgreSQL", error)
finally:
    if connection:
        cursor.close()
        connection.close()
        print("Соединение с PostgreSQL закрыто")
