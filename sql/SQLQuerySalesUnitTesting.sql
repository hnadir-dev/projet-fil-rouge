--Unit Testing TSQLt
-- Example: Creating a test class
EXEC tSQLt.NewTestClass 'SalesTests';

------------------------------------------------------+
--                  fact_sales                        +
------------------------------------------------------+
DROP PROCEDURE SalesTests.[test that sales data exists]
GO
CREATE PROCEDURE SalesTests.[test that sales data exists]
AS
BEGIN
    -- Arrange: (Optional) Set up any necessary data for the test

    -- Act: Perform the action that you want to test
    DECLARE @rowCount INT;
    SELECT @rowCount = COUNT(*) FROM dbo.fact_sales;

    -- Assert: Use tSQLt.Assert functions to check conditions
    -- Check if there is at least one row in the fact_sales table
    EXEC tSQLt.AssertNotEquals 0, @rowCount, 'No sales data found in fact_sales table';

END;

------------------------------------------------------+
--                  dim_products                      +
------------------------------------------------------+
-- Checks data is inserted into dim_product
DROP PROCEDURE SalesTests.[test that products data exists]
GO
CREATE PROCEDURE SalesTests.[test that products data exists]
AS
BEGIN
    -- Arrange: (Optional) Set up any necessary data for the test

    -- Act: Perform the action that you want to test
    DECLARE @rowCount INT;
    SELECT @rowCount = COUNT(*) FROM dbo.dim_products;

    -- Assert: Use tSQLt.Assert functions to check conditions
    -- Check if there is at least one row in the fact_sales table
    EXEC tSQLt.AssertNotEquals 0, @rowCount, 'No sales data found in fact_sales table';

END;

--Test case checks for the uniqueness of product identifiers.
DROP PROCEDURE SalesTests.[test dim_products uniqueness]
GO
CREATE PROCEDURE SalesTests.[test dim_products uniqueness]
AS
BEGIN
    -- Arrange: No specific arrangement needed

    -- Act: Check for uniqueness of product identifiers
    DECLARE @duplicateProductIdCount INT;

    -- Check for duplicate product identifiers
    SELECT @duplicateProductIdCount = COUNT(*)
    FROM (
        SELECT sku, COUNT(*) AS CountOfProductId
        FROM dim_products
        GROUP BY sku
        HAVING COUNT(*) > 1
    ) AS DuplicateProductIds;

    -- Assert: Ensure that product identifiers are unique
    EXEC tSQLt.AssertEquals 0, @duplicateProductIdCount, 'Duplicate product identifiers found in dim_products table';

END;

-- Test case checks for foreign key relationships.
DROP PROCEDURE SalesTests.[test dim_products relationships]
GO
CREATE PROCEDURE SalesTests.[test dim_products relationships]
AS
BEGIN
    -- Arrange: No specific arrangement needed

    -- Act: Check for foreign key relationships
    DECLARE @invalidForeignKeyCount INT;

    -- Check for invalid foreign key relationships
    SELECT @invalidForeignKeyCount = COUNT(*)
    FROM dim_products p
    LEFT JOIN fact_sales fs ON p.sku = fs.sku
    WHERE fs.sku IS NULL;

    -- Assert: Ensure that foreign key relationships are correctly set up
    EXEC tSQLt.AssertEquals 0, @invalidForeignKeyCount, 'Invalid foreign key relationships found in dim_products table';

END;

--Test case checks for NULL values in critical columns.
DROP PROCEDURE SalesTests.[test dim_products null values]
GO
CREATE PROCEDURE SalesTests.[test dim_products null values]
AS
BEGIN
    -- Arrange: No specific arrangement needed

    -- Act: Check for NULL values in critical columns
    DECLARE @nullCount INT;

    -- Check for NULL values in critical columns (adjust column names)
    SELECT @nullCount = COUNT(*)
    FROM dim_products
    WHERE sku IS NULL
       OR name IS NULL
       OR typeID IS NULL
       OR formatID IS NULL
       OR classID IS NULL
       OR subclassID IS NULL
       OR departmentID IS NULL
       OR genreID IS NULL;

    -- Assert: Ensure that no critical columns have NULL values
    EXEC tSQLt.AssertEquals 0, @nullCount, 'NULL values found in critical columns of dim_products table';

END;

------------------------------------------------------+
--                  dim_type                          +
------------------------------------------------------+
DROP PROCEDURE SalesTests.[test dim_type uniqueness]
GO
CREATE PROCEDURE SalesTests.[test dim_type uniqueness]
AS
BEGIN
    -- Arrange: No specific arrangement needed

    -- Act: Check for duplicate values in dim_type
    DECLARE @duplicateCount INT;
    SELECT @duplicateCount = COUNT(*)
    FROM (
        SELECT type, COUNT(*) AS CountOfType
        FROM dim_type
        GROUP BY type
        HAVING COUNT(*) > 1
    ) AS DuplicateTypes;

    -- Assert: Ensure that there are no duplicate values
    EXEC tSQLt.AssertEquals 0, @duplicateCount, 'Duplicate values found in dim_type table';

END;

------------------------------------------------------+
--                  dim_format                        +
------------------------------------------------------+
-- Example: Test case for dim_format table
DROP PROCEDURE SalesTests.[test dim_format data completeness]
GO
CREATE PROCEDURE SalesTests.[test dim_format data completeness]
AS
BEGIN
    -- Arrange: No specific arrangement needed

    -- Act: Check the number of rows and NULL values in dim_format
    DECLARE @rowCount INT, @nullCount INT;

    -- Check the number of rows
    SELECT @rowCount = COUNT(*) FROM dim_format;

    -- Check for NULL values in important columns
    SELECT @nullCount = COUNT(*)
    FROM dim_format
    WHERE formatID IS NULL
       OR [format] IS NULL; -- Adjust column names as needed

    -- Assert: Ensure the expected number of rows and no NULL values
    EXEC tSQLt.AssertEquals 13 , @rowCount, 'Unexpected number of rows in dim_format table';
    EXEC tSQLt.AssertEquals 0, @nullCount, 'NULL values found in important columns of dim_format table';

END;

------------------------------------------------------+
--                  dim_class                         +
------------------------------------------------------+
-- Example: Test case for dim_class table
DROP PROCEDURE SalesTests.[test dim_class data completeness]
GO
CREATE PROCEDURE SalesTests.[test dim_class data completeness]
AS
BEGIN
    -- Arrange: No specific arrangement needed

    -- Act: Check the number of rows and NULL values in dim_class
    DECLARE @rowCount INT, @nullCount INT;

    -- Check the number of rows
    SELECT @rowCount = COUNT(*) FROM dim_class;

    -- Check for NULL values in important columns
    SELECT @nullCount = COUNT(*)
    FROM dim_class
    WHERE classID IS NULL
       OR class IS NULL; -- Adjust column names as needed

    -- Assert: Ensure the expected number of rows and no NULL values
    EXEC tSQLt.AssertEquals 65 , @rowCount, 'Unexpected number of rows in dim_class table';
    EXEC tSQLt.AssertEquals 0, @nullCount, 'NULL values found in important columns of dim_class table';

END;

------------------------------------------------------+
--                  dim_subclass                      +
------------------------------------------------------+
-- Example: Test case for dim_subclass table
DROP PROCEDURE SalesTests.[test dim_subclass data completeness]
GO
CREATE PROCEDURE SalesTests.[test dim_subclass data completeness]
AS
BEGIN
    -- Arrange: No specific arrangement needed

    -- Act: Check the number of rows and NULL values in dim_subclass
    DECLARE @rowCount INT, @nullCount INT;

    -- Check the number of rows
    SELECT @rowCount = COUNT(*) FROM dim_subclass;

    -- Check for NULL values in important columns
    SELECT @nullCount = COUNT(*)
    FROM dim_subclass
    WHERE subclassID IS NULL
       OR subclass IS NULL; -- Adjust column names as needed

    -- Assert: Ensure the expected number of rows and no NULL values
    EXEC tSQLt.AssertEquals 173 , @rowCount, 'Unexpected number of rows in dim_subclass table';
    EXEC tSQLt.AssertEquals 0, @nullCount, 'NULL values found in important columns of dim_subclass table';

END;

------------------------------------------------------+
--                  dim_department                    +
------------------------------------------------------+
-- Example: Test case for dim_department table
DROP PROCEDURE SalesTests.[test dim_department data completeness]
GO
CREATE PROCEDURE SalesTests.[test dim_department data completeness]
AS
BEGIN
    -- Arrange: No specific arrangement needed

    -- Act: Check the number of rows and NULL values in dim_department
    DECLARE @rowCount INT, @nullCount INT;

    -- Check the number of rows
    SELECT @rowCount = COUNT(*) FROM dim_department;

    -- Check for NULL values in important columns
    SELECT @nullCount = COUNT(*)
    FROM dim_department
    WHERE departmentID IS NULL
       OR department IS NULL; -- Adjust column names as needed

    -- Assert: Ensure the expected number of rows and no NULL values
    EXEC tSQLt.AssertEquals 12 , @rowCount, 'Unexpected number of rows in dim_department table';
    EXEC tSQLt.AssertEquals 0, @nullCount, 'NULL values found in important columns of dim_department table';

END;

------------------------------------------------------+
--                     dim_genre                      +
------------------------------------------------------+
-- Example: Test case for dim_genre table
DROP PROCEDURE SalesTests.[test dim_genre data completeness]
GO
CREATE PROCEDURE SalesTests.[test dim_genre data completeness]
AS
BEGIN
    -- Arrange: No specific arrangement needed

    -- Act: Check the number of rows and NULL values in dim_genre
    DECLARE @rowCount INT, @nullCount INT;

    -- Check the number of rows
    SELECT @rowCount = COUNT(*) FROM dim_genre;

    -- Check for NULL values in important columns
    SELECT @nullCount = COUNT(*)
    FROM dim_genre
    WHERE genreID IS NULL
       OR genre IS NULL;

    -- Assert: Ensure the expected number of rows and no NULL values
    EXEC tSQLt.AssertEquals 80 , @rowCount, 'Unexpected number of rows in dim_genre table';
    EXEC tSQLt.AssertEquals 0, @nullCount, 'NULL values found in important columns of dim_genre table';

END;


--------------------------------------------------------------------+
--                      Run Unit Testing                            +
--------------------------------------------------------------------+

EXEC tSQLt.Run 'SalesTests';
