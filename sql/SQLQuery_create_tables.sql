---dimension type
IF OBJECT_ID('dim_type', 'U') IS NOT NULL
BEGIN
    DROP TABLE dim_type;
END;

CREATE TABLE dim_type (
    typeID INT PRIMARY KEY,
    [type] VARCHAR(255),
)

---dimension format
IF OBJECT_ID('dim_format', 'U') IS NOT NULL
BEGIN
    DROP TABLE dim_format;
END;
CREATE TABLE dim_format (
    formatID INT PRIMARY KEY,
    [format] VARCHAR(255),
)
---dimension class
IF OBJECT_ID('dim_class', 'U') IS NOT NULL
BEGIN
    DROP TABLE dim_class;
END;
CREATE TABLE dim_class (
    classID INT PRIMARY KEY,
    [class] VARCHAR(255),
)
---dimension subclass
IF OBJECT_ID('dim_subclass', 'U') IS NOT NULL
BEGIN
    DROP TABLE dim_subclass;
END;
CREATE TABLE dim_subclass (
    subclassID INT PRIMARY KEY,
    [subclass] VARCHAR(255),
)
---dimension department
IF OBJECT_ID('dim_department', 'U') IS NOT NULL
BEGIN
    DROP TABLE dim_department;
END;
CREATE TABLE dim_department (
    departmentID INT PRIMARY KEY,
    [department] VARCHAR(255),
)
---dimension genre
IF OBJECT_ID('dim_genre', 'U') IS NOT NULL
BEGIN
    DROP TABLE dim_genre;
END;
CREATE TABLE dim_genre (
    genreID INT PRIMARY KEY,
    [genre] VARCHAR(255),
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
    typeID INT FOREIGN KEY REFERENCES dim_type(typeID),
    formatID INT FOREIGN KEY REFERENCES dim_format(formatID),
    classID INT FOREIGN KEY REFERENCES dim_class(classID),
    subclassID INT FOREIGN KEY REFERENCES dim_subclass(subclassID),
    departmentID INT FOREIGN KEY REFERENCES dim_department(departmentID),
    genreID INT FOREIGN KEY REFERENCES dim_genre(genreID)
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

-- dim_type 1
-- dim_format 1
-- dim_class 1
-- dim_subclass 1
-- dim_department 1
-- dim_genre 1
-- dim_products 1
-- dim_customers 1
-- dim_date 1
-- dim_paymentMethod 1
-- fact_sales 1

select TOP 100 * from fact_sales where discount > 0

select TOP 100 * from dim_customers

select count(*) from fact_sales,dim_customers
where fact_sales.customerID = dim_customers.customerID
and dim_customers.customerGender = 'male'


select TOP 5 name ,totalPrice from dim_products, fact_sales
where dim_products.sku = fact_sales.sku
and fact_sales.totalPrice = (SELECT MAX(totalPrice) from fact_sales)


select * from fact_sales
order by totalPrice desc

select name, quantitySold, salePrice ,totalPrice from dim_products, fact_sales
where dim_products.sku = fact_sales.sku
order by salePrice desc

select dim_products.sku, sum(quantitySold) as quantitySold from dim_products, fact_sales
where fact_sales.sku = dim_products.sku
group by dim_products.sku
order by sum(quantitySold) desc

select TOP 100 * from fact_sales

select * from dim_products

select * from dim_department

select column_name FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'dim_products';

SELECT TABLE_NAME
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_TYPE = 'bestbuyDataWarehouse';

select * from dim_date
where month(saleDate) = 2