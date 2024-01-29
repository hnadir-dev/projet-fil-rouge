use bestbuyDataWarehouse

-- Table dim_type
CREATE NONCLUSTERED INDEX IX_dim_class ON dim_class(class);
-- Table dim_customer
CREATE NONCLUSTERED INDEX IX_dim_customer_name ON dim_customers(customerName);
-- Table dim_date
CREATE NONCLUSTERED INDEX IX_dim_date ON dim_date(saleDate);
-- Table dim_department
CREATE NONCLUSTERED INDEX IX_dim_department_name ON dim_department(department);
-- Table dim_format
CREATE NONCLUSTERED INDEX IX_dim_format ON dim_format([format]);
-- Table dim_genre
CREATE NONCLUSTERED INDEX IX_dim_genre ON dim_genre(genre);
-- Table dim_paymentMethod
CREATE NONCLUSTERED INDEX IX_dim_paymentMethod ON dim_paymentMethod(paymentMethod);
-- Table dim_product
CREATE NONCLUSTERED INDEX IX_dim_products_name ON dim_products([name]);
CREATE NONCLUSTERED INDEX IX_dim_products_template ON dim_products(productTemplate);
-- Table dim_subclass
CREATE NONCLUSTERED INDEX IX_dim_subclass ON dim_subclass(subclass);
-- Table dim_type
CREATE NONCLUSTERED INDEX IX_dim_type ON dim_type([type]);
-- Table fact_sales
CREATE NONCLUSTERED INDEX IX_fact_sales ON fact_sales(id);
