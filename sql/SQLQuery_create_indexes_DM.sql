use bestbuyDataMart


-- Table dim_customer
CREATE NONCLUSTERED INDEX IX_dim_customer_name ON dim_customers(customerName);

-- Table dim_date
CREATE NONCLUSTERED INDEX IX_dim_date ON dim_date(saleDate);

-- Table dim_department
CREATE NONCLUSTERED INDEX IX_dim_department_name ON dim_department(department);

-- Table dim_paymentMethod
CREATE NONCLUSTERED INDEX IX_dim_paymentMethod ON dim_paymentMethod(paymentMethod);

-- Table dim_product
CREATE NONCLUSTERED INDEX IX_dim_products_name ON dim_products([name]);
CREATE NONCLUSTERED INDEX IX_dim_products_template ON dim_products(productTemplate);

-- Table fact_sales
CREATE NONCLUSTERED INDEX IX_fact_sales ON fact_sales(id);