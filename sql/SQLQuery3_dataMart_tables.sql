--DataMart tables

CREATE DATABASE bestbuyDataMart

use bestbuyDataMart


---dimension department
IF OBJECT_ID('dim_department', 'U') IS NOT NULL
BEGIN
    DROP TABLE dim_department;
END;
CREATE TABLE dim_department (
    departmentID INT PRIMARY KEY,
    [department] VARCHAR(255),
)

---dimension customers
IF OBJECT_ID('dim_customers', 'U') IS NOT NULL
BEGIN
    DROP TABLE dim_customers;
END;
CREATE TABLE dim_customers (
    customerID INT PRIMARY KEY,
    customerName VARCHAR(255),
    customerAge INT,
    customerGender VARCHAR(255),
    customuerEmail TEXT,
    customerCountry VARCHAR(255)
)

---dimension date
IF OBJECT_ID('dim_date', 'U') IS NOT NULL
BEGIN
    DROP TABLE dim_date;
END;
CREATE TABLE dim_date (
    dateID INT PRIMARY KEY,
    saleDate DATETIME,
    --[year] INT,
    --[month] INT,
    --[day] INT,
    --[hour] INT,
    --[minute] INT
)
---dimension paymentMethod
IF OBJECT_ID('dim_paymentMethod', 'U') IS NOT NULL
BEGIN
    DROP TABLE dim_paymentMethod;
END;
CREATE TABLE dim_paymentMethod (
    paymentMethodID INT PRIMARY KEY,
    paymentMethod VARCHAR(255),
)

---dimension products
IF OBJECT_ID('dim_products', 'U') IS NOT NULL
BEGIN
    DROP TABLE dim_products;
END;
CREATE TABLE dim_products (
    sku VARCHAR(255) PRIMARY KEY,
    name VARCHAR(255),
    startDate DATETIME,
    new BIT,
    active BIT,
    activeUpdateDate DATETIME,
    priceUpdateDate DATETIME,
    digital BIT,
    preowned BIT,
    url VARCHAR(255),
    productTemplate VARCHAR(255),
    freeShipping BIT,
    specialOrder BIT,
    mpaaRating VARCHAR(255),
    image VARCHAR(255),
    shortDescription TEXT,
    condition VARCHAR(255),
    customerTopRated BIT,
    customerReviewCount INT,
    customerReviewAverage FLOAT,
    departmentID INT FOREIGN KEY REFERENCES dim_department(departmentID),
)

---fact sales
IF OBJECT_ID('fact_sales', 'U') IS NOT NULL
BEGIN
    DROP TABLE fact_sales;
END;
CREATE TABLE fact_sales (
    id INT PRIMARY KEY IDENTITY,
    sku VARCHAR(255) FOREIGN KEY REFERENCES dim_products(sku),
    regularPrice FLOAT,
    salePrice FLOAT,
    shippingCost FLOAT,
    quantitySold INT,
    totalPrice FLOAT,
    discount FLOAT,
    customerID INT FOREIGN KEY REFERENCES dim_customers(customerID),
    paymentMethodID INT FOREIGN KEY REFERENCES dim_paymentMethod(paymentMethodID),
    dateID INT FOREIGN KEY REFERENCES dim_date(dateID)
);


select sku, count(*) from fact_sales
group by sku


select TOP 100 p.sku, p.[name], c.customerName ,sum(fs.totalPrice) as total from dim_products p
inner join fact_sales fs on fs.sku = p.sku
inner join dim_customers c on fs.customerID = c.customerID
group by p.sku, p.[name], c.customerName
order by total desc


select sum(totalPrice) from fact_sales