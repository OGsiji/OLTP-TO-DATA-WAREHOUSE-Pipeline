from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import FloatType
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

class SparkQualityCheck:

    def __init__(self, spark, cleaned_df):
        self.spark = spark
        self.cleaned_df = cleaned_df

    def check_null_values(self):
        """ Check for null values in the DataFrame """
        null_counts = self.cleaned_df.select([col(c).alias(c) for c in self.cleaned_df.columns]).\
            select([col(c).isNull().cast('int').alias(c) for c in self.cleaned_df.columns]).\
            describe().toPandas().transpose()

        return null_counts

    def check_data_quality(self):
        """ Check for data quality issues in the DataFrame """
        # You can add more data quality checks as needed
        quality_checks = {
            "total_rows": self.cleaned_df.count(),
            "unique_application_ids": self.cleaned_df.select("Application_ID").distinct().count(),
            "unique_transaction_ids": self.cleaned_df.select("Transaction_ID").distinct().count(),
            "unique_transaction_ids": self.cleaned_df.select("Customer_ID").distinct().count(),
            # Add more checks as needed
        }

        return quality_checks
    
    # Read existing data from OLAP data warehouse to identify the latest fact_id
    def get_latest_fact_id(self):
        # Assuming your fact table in OLAP has a 'fact_id' column
        olap_df = spark.read.jdbc(
            url=olap_connection_properties['url'],
            table='your_fact_table',
            properties=olap_connection_properties
        )

        # Find the maximum fact_id to determine the latest one
        max_fact_id = olap_df.agg(F.max('fact_id')).collect()[0][0]

        return max_fact_id
    
# Read existing data from OLAP data warehouse to identify the latest fact_id
def get_data_to_test():
    # Assuming your fact table in OLAP has a 'fact_id' column
    olap_df = spark.read.jdbc(
        url=olap_connection_properties['url'],
        table='your_fact_table',
        properties=olap_connection_properties
    )

    # Find the maximum fact_id to determine the latest one
    max_fact_id = olap_df.agg(F.max('fact_id')).collect()[0][0]

    return max_fact_id


def  Quality_Check():
    # Initialize Spark session
    spark = SparkSession.builder.appName("SparkETL").getOrCreate()

    # Load your data into a PySpark DataFrame (replace 'your_data_path' with the actual path)
    df = get_data_to_test()

    # Create SparkETL instance
    spark_etl = SparkETL(spark)

    # Clean and validate the DataFrame
    cleaned_df = spark_etl.clean_and_validate(df)

    # Show the cleaned DataFrame
    cleaned_df.show()


    

# Assuming you have already loaded and cleaned the data
# Initialize Spark session
spark = SparkSession.builder.appName("QualityCheck").getOrCreate()

# Load your cleaned data into a PySpark DataFrame (replace 'cleaned_data_path' with the actual path)
cleaned_df = spark.read.format("parquet").load("cleaned_data_path")

# Create SparkQualityCheck instance
quality_check = SparkQualityCheck(spark, cleaned_df)

# Perform quality checks
null_counts = quality_check.check_null_values()
data_quality_checks = quality_check.check_data_quality()

# Show the results
print("Null Value Counts:")
null_counts.show(truncate=False)

print("\nData Quality Checks:")
for key, value in data_quality_checks.items():
    print(f"{key}: {value}")

if __name__ == '__main__':

# Initialize Spark session
spark = SparkSession.builder.appName("SparkETL").getOrCreate()


# Create SparkETL instance
spark_etl = SparkETL(spark)

# Clean and validate the DataFrame
cleaned_df = spark_etl.clean_and_validate(df)

# Show the cleaned DataFrame
cleaned_df.show()
