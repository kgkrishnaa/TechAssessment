from datetime import datetime
from pyspark.sql import SparkSession
from google.cloud import storage, bigquery
from pyspark.sql.types import IntegerType, FloatType
from pyspark.sql.functions import col, lit, when, to_date, current_date, regexp_replace

# Initialize Spark session
spark = SparkSession.builder.appName("MyApp").getOrCreate()

# Step 1: Read Data from CSV stored in GCS
def read_data_from_gcs(gcs_path):
    try:
        # Parse GCS bucket and blob names from the gcs_path
        client = storage.Client()
        bucket_name, blob_name = parse_gcs_path(gcs_path)
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_name)

        # Download the file content as string
        data = blob.download_as_string().decode('utf-8')

        # Load the content into a Spark DataFrame directly
        rdd = spark.sparkContext.parallelize([data])
        df = spark.read.option("header", True) \
                        .option("inferSchema", True) \
                        .option("lineSep", "\r") \
                        .csv(gcs_path)

        print("Data read successfully from GCS!")
        df.show(5)
        return df
    except Exception as e:
        print(f"Error reading CSV file from GCS: {e}")
        return None

# Function to parse the GCS path "gs://your_bucket/your_file_path"
def parse_gcs_path(gcs_path):
    if gcs_path.startswith("gs://"):
        parts = gcs_path[5:].split("/", 1)
        bucket_name = parts[0]
        blob_name = parts[1] if len(parts) > 1 else ""
        return bucket_name, blob_name
    raise ValueError("Invalid GCS path. Must start with 'gs://'.")
 
# Step 2: Clean Data using PySpark
def clean_data(df):
    # Remove any '\r' characters from all string columns
    for col_name in df.columns:
        # Apply regexp_replace to remove '\r' in string columns
        df = df.withColumn(col_name, regexp_replace(col(col_name), r'\r', ''))
    
    #Ensure Order ID is Integer
    df = df.withColumn("OrderID", col("OrderID").cast("integer"))
    df = df.withColumn("CustomerId", col("CustomerId").cast("integer"))
    df = df.withColumn("ProductID", col("ProductID").cast("integer"))
    #df = df.withColumn("ProductID", lit(None).cast( IntegerType()) )    

    # Convert 'OrderDate' to valid date format and fill missing values with the current date
    df = df.withColumn("OrderDate", to_date(col("OrderDate"), "yyyy-MM-dd")) \
           .withColumn("OrderDate", when(col("OrderDate").isNull(), current_date()).otherwise(col("OrderDate")))

    # Replace "One Hundred Pounds" in 'OrderAmount' with 100 and convert the column to numeric
    df = df.withColumn("OrderAmount", when(col("OrderAmount") == "One Hundred Pounds", lit(100)).otherwise(col("OrderAmount"))) \
            .withColumn("OrderAmount", col("OrderAmount").cast(FloatType()))

    # Replace "Three" in 'Quantity' with 3 and convert the column to numeric
    df = df.withColumn("Quantity", when(col("Quantity") == "Three", lit(3)).otherwise(col("Quantity"))) \
           .withColumn("Quantity", col("Quantity").cast(IntegerType()))

    # Handle missing ProductID and Quantity
    df = df.withColumn("ProductID", when(col("ProductID").isNull(), lit(1000)).otherwise(col("ProductID"))) 
    df = df.withColumn("Quantity", when(col("Quantity").isNull(), lit(0)).otherwise(col("Quantity")))
    df = df.withColumn("OrderAmount", when(col("OrderAmount").isNull(), lit(0)).otherwise(col("OrderAmount")))
    
    # Remove duplicate rows
    df = df.dropDuplicates()

    print("Data cleaned successfully!")
    df.show(5)
    return df

# Step 3: Save cleaned data to GCS
def save_to_gcs(df, gcs_path):
    try:
        # Write the DataFrame directly to GCS in CSV format
        df.write \
          .option("header", True) \
          .mode("overwrite") \
          .csv(gcs_path)

        print(f"File successfully written to GCS at: {gcs_path}")
    except Exception as e:
        print(f"Error saving to GCS: {e}")

# Step 4: Write cleaned data to BigQuery
def write_to_bq(df, project_id, dataset_id, table_id):
    try:
        # Convert PySpark DataFrame to Pandas DataFrame for BigQuery upload
        df_pandas = df.toPandas()

        # Use BigQuery client to upload data
        client = bigquery.Client(project=project_id)
        table_ref = f"{project_id}.{dataset_id}.{table_id}"
       
        # Load the dataframe to BigQuery table
        job = client.load_table_from_dataframe(df_pandas, table_ref)
        job.result()  # Wait for the job to complete
       
        print(f"Data successfully written to BigQuery table: {table_ref}")
    except Exception as e:
        print(f"Error writing to BigQuery: {e}")

# Main function
def main():
    # GCS file path (for example: 'gs://your_bucket/order.csv')
    gcs_path = 'gs://myproject_landing/order.csv'

    # Step 1: Read the data from GCS
    df = read_data_from_gcs(gcs_path)
    if df is not None:
        # Step 2: Clean the data
        cleaned_df = clean_data(df)

        # Add a new 'OrderValue' column calculated from 'OrderAmount' * 'Quantity'
        cleaned_df = cleaned_df.withColumn("OrderValue", col("OrderAmount") * col("Quantity"))

        # Step 3: Save cleaned data to GCS
        #bucket_name = 'myproject_staging'
        #destination_blob_name = 'cleaned_order.csv'
        gcs_output_path='gs://myproject_staging/cleaned_order.csv'
        save_to_gcs(cleaned_df, gcs_output_path)

        # Step 4: Write cleaned data to BigQuery
        project_id = 'plasma-ripple-261523'
        dataset_id = 'DataSet_Orders'
        table_id = 'Orders'
        write_to_bq(cleaned_df, project_id, dataset_id, table_id)

if __name__ == '__main__':
    main()


##
#Task 1.1 - Line 107 - df = read_data_from_gcs(gcs_path) 
#Task 1.2.1 - Remove Duplicates -  Line 66 - df.drop_duplicates(inplace=True)
#Task 1.2.2 - Fill missing value - .fillna methods on order data / product id / quantity 
# 
##