from airflow import DAG
from airflow.operators.python_operator import PythonOperator

import subprocess

from sqlalchemy import create_engine, text
import pandas as pd

from datetime import datetime, timedelta

from cryptography.fernet import Fernet

# Define default arguments for the DAG
default_args = {
    'owner': 'NADIR Hicham',
    'start_date': datetime(2024, 1, 17),
    'retry_dely': timedelta(seconds=10),
    'end_date':None, 
}

connection_string_stg = "mssql+pyodbc://Admin:admin1234@192.168.56.1/bestbuyStagingAreadb?driver=ODBC+Driver+17+for+SQL+Server"
connection_string_dw = "mssql+pyodbc://Admin:admin1234@192.168.56.1/bestbuyDataWarehouse?driver=ODBC+Driver+17+for+SQL+Server"

# Create the SQLAlchemy Engine
engine = create_engine(connection_string_stg)

dateNow = datetime.now().date()
year = 2023#dateNow.year
month = dateNow.month

month_mapping = {
    'janv': '01',
    'févr': '02',
    'mars': '03',
    'avr': '04',
    'mai': '05',
    'juin': '06',
    'juil': '07',
    'août': '08',
    'sept': '09',
    'oct': '10',
    'nov': '11',
    'déc': '12'
}

def runDataTransformation():
    
    with engine.connect() as conn:
        #SQlServer Data
        result = conn.execute(text(f"select * from sales where YEAR(saleDate) = '{year}' and MONTH(saleDate) = '{month}'"))
        columns = result.keys() # Extract column names
        sql_df = pd.DataFrame(result.fetchall(), columns=columns)
        #Transformation date "mai 16 2023 11:54AM"
        # Replace the French month name with the numerical representation
        sql_df['saleDate'] =  sql_df['saleDate'].replace(month_mapping,regex=True)
        #sql_df['saleDate'] = pd.to_datetime(sql_df['saleDate'].replace(month_mapping, regex=True), format='%b %d %Y %I:%M%p', errors='coerce')

        #CSV Data
        csv_df = pd.read_csv('/mnt/c/users/youcode/desktop/projet-fil-rouge/resources/sales.csv', low_memory=False)
        csv_df['saleDate'] = pd.to_datetime(csv_df['saleDate'])
        csv_df = csv_df[csv_df['saleDate'].dt.strftime('%Y-%m') == f'{year}-{month if month > 9 else "0"+str(month)}']

        #Json Data
        json_df = pd.read_json('/mnt/c/users/youcode/desktop/projet-fil-rouge/resources/sales.json')
        json_df['saleDate'] = pd.to_datetime(json_df['saleDate'], unit='ms')
        json_df = json_df[json_df['saleDate'].dt.strftime('%Y-%m') == f'{year}-{month if month > 9 else "0"+str(month)}']

        #data concatenation
        sales_data = pd.concat([sql_df,csv_df,json_df])

        
        #+--------------------------------------------------------------------------+
        #                            Data Transformation                            #
        #+--------------------------------------------------------------------------+

        # * Drop duplicate rows
        sales_data.drop_duplicates(inplace=True)

        # * Replace missing values in 'customerReviewCount' with 0
        sales_data['customerReviewCount'] = sales_data['customerReviewCount'].fillna(0)

        # * Convert 'customerAge' to numeric and handle missing values
        sales_data['customerAge'] = pd.to_numeric(sales_data['customerAge'], errors='coerce')

        # * Convert 'customerGender' to lowercase for consistency
        sales_data['customerGender'] = sales_data['customerGender'].str.lower()

        # * Remove leading and trailing whitespaces in 'customerName'
        sales_data['customerName'] = sales_data['customerName'].str.strip()

        # * Filter out rows where 'quantitySold' is less than or equal to 0
        sales_data = sales_data[sales_data['quantitySold'] > 0]

        # * Replace missing values in 'format' with NoFormat
        sales_data['format'].fillna('NoFormat',inplace=True)
        
        # * Replace missing values in 'genre' with NoGenre
        sales_data['genre'].fillna('NoGenre',inplace=True)

        # * Create a new column 'discount' representing the discount percentage
        sales_data['discount'] = ((sales_data['regularPrice'].astype(float) - sales_data['salePrice'].astype(float)) / sales_data['regularPrice'].astype(float)) * 100

        #Encrypting sensitive user data 
        # Generate a key for encryption (keep this key secure)
        encryption_key = Fernet.generate_key()
        cipher_suite = Fernet(encryption_key)

        sales_data['customuerEmail'] = sales_data['customuerEmail'].apply(lambda x: cipher_suite.encrypt(x.encode('utf-8')).decode('utf-8'))
        

        #+-------------------------------------------------------------------------+
        #                           Data Modeling                                  #
        #+-------------------------------------------------------------------------+

        # * Dataframe Type
        df_type = pd.DataFrame(sales_data['type'].unique()).rename(columns={0:'type'})
        df_type.insert(0,"typeID",[id for id in range(1,len(df_type['type'])+1)])
        
        # * Dataframe format
        df_format = pd.DataFrame(sales_data['format'].unique()).rename(columns={0:'format'})
        df_format.insert(0,"formatID",[id for id in range(1,len(df_format['format'])+1)])

        # * Dataframe calss
        df_class = pd.DataFrame(sales_data['class'].unique()).rename(columns={0:'class'})
        df_class.insert(0,"classID",[id for id in range(1,len(df_class['class'])+1)])

        # * Dataframe subcalss
        df_subclass = pd.DataFrame(sales_data['subclass'].unique()).rename(columns={0:'subclass'})
        df_subclass.insert(0,"subclassID",[id for id in range(1,len(df_subclass['subclass'])+1)])

        # * Dataframe department
        df_department =  pd.DataFrame(sales_data['department'].unique()).rename(columns={0:'department'})
        df_department.insert(0,'departmentID',[id for id in range(1,len(df_department['department'])+1)])

        # * Dataframe Genre
        df_genre = pd.DataFrame(sales_data['genre'].unique()).rename(columns={0:'genre'})
        df_genre.insert(0,'genreID',[id for id in range(1,len(df_genre['genre'])+1)])

        # * Dataframe PaymentMEthod 
        df_payment = pd.DataFrame(sales_data['paymentMethod'].unique()).rename(columns={0:'paymentMethod'})
        df_payment.insert(0,'paymentMethodID',[id for id in range(1,len(df_payment['paymentMethod'])+1)])

        # * Dataframe Customers
        df_customers = sales_data[['customerName', 'customerAge', 'customerGender', 'customuerEmail','customerCountry']].drop_duplicates()
        df_customers.insert(0,'customerID',[id for id in range(1,len(df_customers)+1)])
        
        # * Dataframe product
        df_products = sales_data[['sku','name','startDate','new','active','activeUpdateDate','priceUpdateDate',
                                    'digital', 'preowned','url', 'productTemplate','freeShipping','specialOrder',
                                    'mpaaRating','image','shortDescription','condition','customerTopRated','customerReviewCount',
                                    'customerReviewAverage','type','format','class','subclass','department','genre']].drop_duplicates()
        #Join tables
        df_products = pd.merge(df_products,df_type,on=['type'],how='left')
        df_products = pd.merge(df_products,df_format,on=['format'],how='left')
        df_products = pd.merge(df_products,df_class,on=['class'],how='left')
        df_products = pd.merge(df_products,df_subclass,on=['subclass'],how='left')
        df_products = pd.merge(df_products,df_department,on=['department'],how='left')
        df_products = pd.merge(df_products,df_genre,on=['genre'],how='left')

        df_products.drop(['type','format','class','subclass','department','genre'], axis=1, inplace=True)

        ##Convert type of columns
        df_products['sku'] = df_products['sku'].astype(str)
        df_products['startDate'] = pd.to_datetime(df_products['startDate'])
        df_products['new'] = df_products['new'].astype(int)
        df_products['active'] = df_products['active'].astype(int)
        df_products['activeUpdateDate'] = pd.to_datetime(df_products['activeUpdateDate'])
        df_products['priceUpdateDate'] = pd.to_datetime(df_products['priceUpdateDate'])
        df_products['digital'] = df_products['digital'].astype(int)
        df_products['preowned'] = df_products['preowned'].astype(int)
        df_products['freeShipping'].fillna(0,inplace=True)
        df_products['freeShipping'] = df_products['freeShipping'].astype(int)
        df_products['specialOrder'] = df_products['specialOrder'].astype(int)
        df_products['customerTopRated'].fillna(0,inplace=True)
        df_products['customerTopRated'] = df_products['customerTopRated'].astype(int)
        df_products['customerReviewCount'].fillna(0,inplace=True)
        df_products['customerReviewCount'] = df_products['customerReviewCount'].astype(int)
        df_products['customerReviewAverage'].fillna(0,inplace=True)
        df_products['customerReviewAverage'] = df_products['customerReviewAverage'].astype(float)
        df_products['shortDescription'].fillna('NoShortDescription',inplace=True)

        # * Dataframe Date
        df_date = sales_data[['saleDate']].drop_duplicates()

        df_date.insert(0,'dateID',[id for id in range(1,len(df_date)+1)])
        df_date['saleDate'] = pd.to_datetime(df_date['saleDate'])
        # df_date.insert(2,'year',df_date['saleDate'].dt.year)
        # df_date.insert(3,'month',df_date['saleDate'].dt.month)
        # df_date.insert(4,'day',df_date['saleDate'].dt.day)
        #df_date.insert(5,'hour',df_date['saleDate'].dt.hour)
        #df_date.insert(6,'minute',df_date['saleDate'].dt.minute)

        # * Dateframe sales
        df_sales = sales_data[['sku','regularPrice', 'salePrice','shippingCost','customerName', 'customerAge', 
                               'customerGender','customuerEmail','customerCountry', 'saleDate', 'quantitySold',
                                 'paymentMethod','totalPrice', 'discount']]#

        #convert date tyep
        df_sales['saleDate'] = pd.to_datetime(df_sales['saleDate'])

        df_sales = pd.merge(df_sales,df_customers,on=['customerName', 'customerAge','customerGender','customuerEmail','customerCountry'],how='left')#
        df_sales = pd.merge(df_sales,df_payment,on=['paymentMethod'],how='left')
        df_sales = pd.merge(df_sales,df_date,on=['saleDate'],how='left')

        df_sales.drop(['customerName', 'customerAge', 'customerGender','customuerEmail','customerCountry','paymentMethod'],axis=1,inplace=True)#
        df_sales.drop(['saleDate'], axis=1,inplace=True)#, 'year', 'month', 'day','hour','minute'

        #transformation type of columns
        df_sales['sku'] = df_sales['sku'].astype(str)
        df_sales['regularPrice'] = df_sales['regularPrice'].astype(float)
        df_sales['salePrice'] = df_sales['salePrice'].astype(float)

        df_sales['shippingCost'].fillna(0,inplace=True)
        df_sales['shippingCost'] = pd.to_numeric(df_sales['shippingCost'])

        #df_sales.insert(0,'id',[id for id in range(1,len(df_sales)+1)])

        #+-----------------------------------------------------------------+
        #       Save the dataframes into tables in datawarehouse           #
        #+-----------------------------------------------------------------+

        engine_dw = create_engine(connection_string_dw)

        #Create and save dimension tables to SQL Server
        fact_dimension_tables = {
            'dim_type': df_type,
            'dim_format': df_format,
            'dim_class': df_class,
            'dim_subclass': df_subclass,
            'dim_department': df_department,
            'dim_genre': df_genre,
            #
            'dim_products': df_products,
            #
            'dim_customers': df_customers,
            #
            'dim_date': df_date,
            #
            'dim_paymentMethod': df_payment,
            #Fact table
            'fact_sales': df_sales
        }

        #Save dimension tables to SQL Server
        for table_name, table_df in fact_dimension_tables.items():
            try:
                table_df.to_sql(table_name, con=engine_dw, if_exists='append', index=False) #SCD 2 : Création d'une nouvelle ligne
            except:
                pass
            
        #debug
        #subprocess.run(f'touch /mnt/c/users/youcode/desktop/projet-fil-rouge/dags/data_inserted_with_successfuly.txt', shell=True)


# Create a DAG
with DAG('data_transformation_process', default_args=default_args, schedule_interval="@once") as dag:#*/1 * * * *
    # Create PythonOperator tasks
    task_1 = PythonOperator(
        task_id='data_transformation',
        python_callable=runDataTransformation,
    )

    
    # tasks
    task_1 #>> task_2 >> task_3

if __name__ == "__main__":
    dag.cli()