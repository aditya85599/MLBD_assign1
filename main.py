from pyspark.sql import SparkSession
from pyspark.sql.functions import (input_file_name, col, collect_list, concat_ws,regexp_extract, length, avg)
# Start Spark session
spark = SparkSession.builder.appName("apache_spark").getOrCreate()

# Load all text files
books_df = spark.read.text("hdfs://localhost:9000/D184MB/*.txt").withColumn("file_name", input_file_name())


# Combine lines into full book text
books_df = books_df.groupBy("file_name").agg(concat_ws("\n", collect_list("value")).alias("text"))

# Extract metadata using regex

# Title
books_df = books_df.withColumn(
    "title",
    regexp_extract("text", r"Title:\s*(.*)", 1)
)

# Release Year (4 digit year)
books_df = books_df.withColumn(
    "release_date",
    regexp_extract("text", r"Release Date:.*?(\d{4})", 1)
)

# Language
books_df = books_df.withColumn(
    "language",
    regexp_extract("text", r"Language:\s*(.*)", 1)
)

# Encoding
books_df = books_df.withColumn(
    "encoding",
    regexp_extract("text", r"Character set encoding:\s*(.*)", 1)
)

# Show extracted metadata
books_df.select("file_name", "title", "release_date", "language", "encoding").show(truncate=False)


# ----------------------
# ANALYSIS PART
# ----------------------
'''

print("Books Released Each Year:")
books_df.groupBy("release_date").count().orderBy("release_date").show()

print("Most Common Language:")
books_df.groupBy("language").count().orderBy(col("count").desc()).show() 
'''

print("Average Title Length:")
books_df.select(avg(length("title"))).show()
