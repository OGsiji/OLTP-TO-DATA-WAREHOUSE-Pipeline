from pyspark.sql import SparkSession
from pyspark.sql.functions import col, abs, substring
from pyspark.sql.types import IntegerType, FloatType
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from pyspark.sql.types import FloatType, IntegerType


class SparkETL:

    def __init__(self, spark):
        self.spark = spark

    def loan_condition(self, emp_length):
        """ Helper method for data cleaning, which replaces employment length with integers """
        return (
            col(emp_length)
            .when(col(emp_length).like('%10+'), 10)
            .when(col(emp_length).like('%< 1 year'), 0)
            .otherwise(col(emp_length))
        )

    def check_if_numeric(self, df):
        """ Method that replaces all non-numeric rows that are not nullable with zeros """
        numeric_cols = ['Loan_Amount', 'Annual_Income', 'Transaction_Amount', 'Balance']

        for col_name in numeric_cols:
            df = df.withColumn(col_name, col(col_name).cast(FloatType())).na.fill(0, subset=[col_name])

        return df

    def convert_to_date(self, df):
        """ Method that converts all text-based data points to datetime objects """
        date_cols = ['Loan_Application_Date', 'Transaction_Date']

        for col_name in date_cols:
            df = df.withColumn(col_name, col(col_name).cast('date'))

        return df

    def convert_to_positive(self, df):
        """ Method that converts all negative numeric data points to positive if there is any """
        numeric_columns = ['Transaction_Amount', 'Balance']

        for col_name in numeric_columns:
            df = df.withColumn(col_name, abs(col(col_name)))

        return df

    def miscellaneous(self, df):
        """ Method that performs miscellaneous data cleaning and validation for some columns of the given dataframe """
        # Replace if there is any n/a field in the dataframe
        df = df.na.replace('n/a', None)

        # Remove all duplicate rows from the given dataframe
        df = df.dropDuplicates()

        # Replacing "employment length" column with integer number as follows:
        # Replace by zero if it is NULL
        # Replace "10+" with "10" and
        # Replace "< 1" with "0" as described in the loan stats dictionary
        # Remove all non-numeric chars
        df = df.withColumn('Employment_Status', self.loan_condition('Employment_Status')).withColumn('Employment_Status', col('Employment_Status').cast(IntegerType()))

        # Remove leading whitespaces and get the integer part of the "term" column
        df = df.withColumn('term', substring(col('term'), 1, 2).cast(IntegerType()))

        # Annual income can not be NULL. Replace those rows with zeros.
        df = df.withColumn('Annual_Income', col('Annual_Income').cast(FloatType())).na.fill(0, subset=['Annual_Income'])

        # Replace NULL values in "verification_status" with 'Not Verified'
        df = df.na.fill('Not Verified', subset=['verification_status'])

        # FUTURE WORK: There are some rows with debt-to-income (dti) ratio equal to zero
        # In order to give a better estimation about dti
        # an estimate dti can be calculated by calculating
        # installment/(annual_inc/12)*100

        # There is an ID as "Loans that do not meet the credit policy".
        # There could be more. Remove them by dropping rows with no
        # member id.
        df = df.filter(col('member_id').isNotNull())

        # delinq_2yrs can not be NULL. Replace NULL values with zeros.
        df = df.withColumn('delinq_2yrs', col('delinq_2yrs').cast(FloatType())).na.fill(0, subset=['delinq_2yrs'])

        # inq_last_6mths can not be NULL. Replace NULL values with zeros.

        return df
    

# Initialize Spark session
spark = SparkSession.builder.appName("YourAppName").getOrCreate()

# Connect to OLTP database
oltp_connection_properties = {
    "driver": "your_database_driver",
    "url": "your_database_url",
    "user": "your_username",
    "password": "your_password"
}

# Connect to OLAP data warehouse
olap_connection_properties = {
    "driver": "your_database_driver",
    "url": "your_olap_database_url",
    "user": "your_username",
    "password": "your_password"
}

# Read existing data from OLAP data warehouse to identify the latest fact_id
def get_latest_fact_id(fact_table):
    # Assuming your fact table in OLAP has a 'fact_id' column
    olap_df = spark.read.jdbc(
        url=olap_connection_properties['url'],
        table=fact_table,
        properties=olap_connection_properties
    )

    # Find the maximum fact_id to determine the latest one
    max_fact_id = olap_df.agg(F.max('fact_id')).collect()[0][0]

    return max_fact_id

# Read incremental data from OLTP database
def read_incremental_from_oltp(table_name, max_fact_id):
    # Assuming your OLTP data does not have a 'fact_id' column
    oltp_df = spark.read.jdbc(
        url=oltp_connection_properties['url'],
        table=table_name,
        properties=oltp_connection_properties
    )

    # Generate surrogate keys for 'fact_id'
    oltp_df_with_keys = oltp_df.withColumn("fact_id", F.monotonically_increasing_id())

    # Filter only the new records (incremental extraction)
    incremental_df = oltp_df_with_keys.filter(F.col('fact_id') > max_fact_id)

    return incremental_df


def clean_and_transform_data(incremental_data):
    etl = SparkETL(spark)
    cleaned_df = etl.check_if_numeric(df)
    cleaned_df = etl.convert_to_date(cleaned_df)
    cleaned_df = etl.convert_to_positive(cleaned_df)
    cleaned_df = etl.miscellaneous(cleaned_df)

    return cleaned_df


# Write the new data with surrogate keys back to OLAP data warehouse
def write_to_warehouse(df_with_keys,your_table):
    df_with_keys.write.jdbc(
        url=olap_connection_properties['url'],
        table=your_table,
        mode='append',
        properties=olap_connection_properties
    )

# Main script
if __name__ == "__main__":
    # Get the latest fact_id from OLAP
    latest_fact_id = get_latest_fact_id()

    # Read incremental data from OLTP and generate surrogate keys
    incremental_data = read_incremental_from_oltp('your_table_name', latest_fact_id)

    cleaned_and_transformed_data = clean_and_transform_data(incremental_data)


    write_to_olap(cleaned_and_transformed_data, 'Loan_Dimension')


