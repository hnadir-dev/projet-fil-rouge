from geopy.geocoders import Nominatim

from confluent_kafka import Producer

from sqlalchemy import create_engine, text
import pandas as pd
import random
import time
from datetime import datetime
import requests
import json
from faker import Faker

faker = Faker()

connection_string = "mssql+pyodbc://Admin:admin1234@LAPTOP-B5O30HDH\SQLEXPRESS/bestbuyDataWarehouse?driver=ODBC+Driver+17+for+SQL+Server"
# Create the SQLAlchemy Engine
engine = create_engine(connection_string)

order_topic = "order-topic"

kafka_config = {
    "bootstrap.servers": "localhost:9092",
}

producer = Producer(kafka_config)

user_agent = "BestBuyApp"
geolocator = Nominatim(user_agent=user_agent)

with engine.connect() as connection:
    result = connection.execute(text('''select TOP 5 dim_products.sku, sum(quantitySold) as quantitySold from dim_products, fact_sales
                                        where fact_sales.sku = dim_products.sku
                                        group by dim_products.sku
                                        order by sum(quantitySold) desc'''))
    columns = result.keys()
    sql_df = pd.DataFrame(result.fetchall(), columns=columns)
    skus = sql_df['sku'].tolist()

    while True:
        sku = random.choice(skus)
        api_key = 'vGrtaHwUGfKD9wqg1pgF6fDm'
        url_details = f"https://api.bestbuy.com/v1/products/{sku}.json?apiKey={api_key}"
        
        response_details = requests.get(url_details)

        if response_details.status_code == 200:
            product_details = response_details.json()
            #Customer info:
            product_details['customerName'] = faker.name()
            product_details['customerAge'] = random.randrange(18,65)
            product_details['customerGender'] = random.choice(['Female', 'Male'])
            product_details['customuerEmail'] = faker.email()
            product_details['customerCountry'] = faker.country()
            location = geolocator.geocode(faker.country())
            if location:
                product_details['location'] = dict(lat=location.latitude, lon=location.longitude)
            else:
                product_details['location'] = (0,0)

            #Sale info :
            # current dateTime
            now = datetime.now()
            product_details['saleDate'] = now.strftime("%m/%d/%Y %I:%M") #random_date("1/1/2023 1:30", "1/1/2024 4:50", random.random())
            #p['saleTime'] = p['saleDate'].split(' ')[1]
            product_details['quantitySold'] = random.randrange(1,3)
            product_details['paymentMethod'] = random.choice(['Credit cards', 'Debit cards','Paypal','Apple Pay','Amazon Pay','Google Pay'])
            product_details['totalPrice'] = product_details['quantitySold'] * product_details['salePrice']

            o = dict(sku=product_details['sku'],name=product_details['name'],type=product_details['type'],startDate=product_details['startDate'],new=product_details['new'],active=product_details['active'],
                                   activeUpdateDate=product_details['activeUpdateDate'],regularPrice=product_details['regularPrice'],salePrice=product_details['salePrice'],
                                   priceUpdateDate=product_details['priceUpdateDate'],digital=product_details['digital'],preowned=product_details['preowned'],url=product_details['url'],
                                   productTemplate=product_details['productTemplate'],customerReviewCount=product_details['customerReviewCount'],customerReviewAverage=product_details['customerReviewAverage'],
                                   customerTopRated=product_details['customerTopRated'],
                                   format=product_details['format'],freeShipping=product_details['freeShipping'],shippingCost=product_details['shippingCost'],
                                   specialOrder=product_details['specialOrder'],shortDescription=product_details['shortDescription'],pclass=product_details['class'],subclass=product_details['subclass'],
                                   department=product_details['department'],mpaaRating=product_details['mpaaRating'],image=product_details['image'],
                                   condition=product_details['condition'],genre=product_details['genre'],customerName=product_details['customerName'],customerAge=product_details['customerAge'],
                                   customerGender=product_details['customerGender'],customuerEmail=product_details['customuerEmail'],
                                   customerCountry=product_details['customerCountry'],location=product_details['location'],saleDate=product_details['saleDate'],
                                   quantitySold=product_details['quantitySold'],paymentMethod=product_details['paymentMethod'],totalPrice=product_details['totalPrice'])
            
            producer.produce(order_topic, key="key", value=json.dumps(o).encode('utf-8'))
            producer.flush()
        else:
            print('Error fetching...')
            
        time.sleep(4)
