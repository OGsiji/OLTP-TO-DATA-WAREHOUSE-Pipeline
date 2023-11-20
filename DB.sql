-- Drop the table if it exists
DROP TABLE IF EXISTS "warehouse schema"."Loan_facts";

-- Create the table in the "warehouse schema"
CREATE TABLE "warehouse schema"."Loan_facts"
(
    FACT_ID SERIAL NOT NULL,
    Application_ID VARCHAR NOT NULL,
    Transaction_ID VARCHAR NOT NULL,
    LOAN_DATE_ID VARCHAR NOT NULL,
    CUSTOMER_ID VARCHAR NOT NULL,
    LOAN_AMOUNT BIGINT NOT NULL,
    BALANCE BIGINT NOT NULL
);

-- Insert data into Loan_facts table
INSERT INTO "warehouse schema"."Loan_facts" ("application_id", "transaction_id", "loan_date_id", "customer_id", "loan_amount", "balance")
SELECT
    p."Application_ID",
    r."Transaction_ID",
    REPLACE("Loan_Application_Date", '-', '') as loan_date_id,
    r."Customer_ID",
    p."Loan_Amount",
    r."Balance"
FROM
    public."Loan_Application_data"  as p
JOIN
    "Customer_Bank_Statement_data" r ON p."Customer_ID" = r."Customer_ID";

-- Display Sample Records from Loan Facts Table
SELECT * FROM "warehouse schema"."Loan_facts" LIMIT 5;

-- Create Transaction Dimension Table
CREATE TABLE "warehouse schema"."Transaction_dim"
(
    TRANSACTION_ID VARCHAR PRIMARY KEY,
    TRANSACTION_DATE DATE NOT NULL,
    TRANSACTION_AMOUNT BIGINT NOT NULL,
    NARRATION VARCHAR NOT NULL
);

-- Insert Data into Transaction Dimension Table
INSERT INTO "warehouse schema"."Transaction_dim" ("transaction_id", "transaction_date", "transaction_amount", "narration")
SELECT
    "Transaction_ID",
    CAST("Transaction_Date" AS DATE),
    "Transaction_Amount",
    "Narration"
FROM
    "Customer_Bank_Statement_data";

-- Display Sample Records from Transaction Dimension Table
SELECT * FROM "warehouse schema"."Transaction_dim" LIMIT 5;

-- Drop the table if it exists
DROP TABLE IF EXISTS "warehouse schema"."Customer_dim";

-- Create Customer Dimension Table
CREATE TABLE "warehouse schema"."Customer_dim"
(
    CUSTOMER_ID VARCHAR NOT NULL,
    APP_ID VARCHAR NOT NULL,
    CREDIT_SCORE INT NOT NULL,
    EMPLOYEE_STATUS VARCHAR NOT NULL,
    ANNUAL_INCOME VARCHAR NOT NULL
);

-- Insert Data into Customer Dimension Table
INSERT INTO "warehouse schema"."Customer_dim" ("customer_id", "app_id", "credit_score", "employee_status", "annual_income")
SELECT
    "Customer_ID",
    "Application_ID",
    "Credit_Score",
    "Employment_Status",
    "Annual_Income"
FROM
    "Loan_Application_data";

-- Display Sample Records from Customer Dimension Table
SELECT * FROM "warehouse schema"."Customer_dim" LIMIT 5;

-- Drop the table if it exists
DROP TABLE IF EXISTS "warehouse schema"."dim_date";

-- Create the table in the "warehouse schema"
CREATE TABLE "warehouse schema"."dim_date"
(
    loan_date_id VARCHAR NOT NULL,
    date_full DATE NOT NULL,
    year BIGINT NOT NULL,
    month VARCHAR NOT NULL,
    quarter VARCHAR NOT NULL,
    week VARCHAR NOT NULL,
    day INT NOT NULL,
    day_of_week INT NOT NULL,
    day_name VARCHAR NOT NULL,
    month_name VARCHAR NOT NULL,
    is_weekday INT NOT NULL,
    is_leapyear INT NOT NULL
);

-- Insert data into the dim_date table
INSERT INTO "warehouse schema"."dim_date" (
    loan_date_id ,
    date_full,
    year,
    quarter,
    month,
    week,
    day,
    day_of_week,
    day_name,
    month_name,
    is_weekday,
    is_leapyear
)
SELECT
    REPLACE("Loan_Application_Date", '-', '') as loan_date_id,
    "Loan_Application_Date"::DATE AS date_full,
    EXTRACT(YEAR FROM TO_DATE("Loan_Application_Date", 'YYYY-MM-DD')) AS year,
    CAST(EXTRACT(QUARTER FROM TO_DATE("Loan_Application_Date", 'YYYY-MM-DD')) AS VARCHAR) AS quarter,
    EXTRACT(MONTH FROM TO_DATE("Loan_Application_Date", 'YYYY-MM-DD')) AS month,
    EXTRACT(WEEK FROM TO_DATE("Loan_Application_Date", 'YYYY-MM-DD')) AS week,
    EXTRACT(DAY FROM TO_DATE("Loan_Application_Date", 'YYYY-MM-DD')) AS day,
    EXTRACT(DOW FROM TO_DATE("Loan_Application_Date", 'YYYY-MM-DD')) AS day_of_week,
    TO_CHAR(TO_DATE("Loan_Application_Date", 'YYYY-MM-DD'), 'Dy') as day_name,
    TO_CHAR(TO_DATE("Loan_Application_Date", 'YYYY-MM-DD'), 'Month') as month_name,
    CASE WHEN EXTRACT(DOW FROM TO_DATE("Loan_Application_Date", 'YYYY-MM-DD')) IN (0, 6) THEN 0 ELSE 1 END AS is_weekday,
    CASE WHEN EXTRACT(ISODOW FROM TO_DATE("Loan_Application_Date", 'YYYY-MM-DD')) = 1 AND EXTRACT(ISODOW FROM TO_DATE("Loan_Application_Date", 'YYYY-MM-DD')) = 366 THEN 1 ELSE 0 END AS is_leapyear
FROM public." Loan_Application_data";

-- Display Sample Records from dim_date Table
SELECT * FROM "warehouse schema"."dim_date" LIMIT 5;

-- Some simple Query Optimization
SELECT
    facts.customer_id AS customer_id,
    trans.transaction_date,
    facts.loan_application_date,
    cust.employee_status,
    trans.narration,
    SUM(trans.transaction_amount) AS total_transaction_amount
FROM
    "warehouse schema"."Customer_dim" AS cust
JOIN
    "warehouse schema"."Loan_facts" AS facts ON facts.customer_id = cust.customer_id
JOIN
    "warehouse schema"."Transaction_dim" AS trans ON facts.transaction_id = trans.transaction_id
GROUP BY
    1, 2, 3, 4, 5
ORDER BY
    customer_id ASC
LIMIT 50;


