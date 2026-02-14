from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, collect_list, flatten, col, expr
from pyspark.ml.feature import Tokenizer, StopWordsRemover, HashingTF, IDF, Normalizer
from pyspark.ml.functions import vector_to_array

# -------------------------
# Spark Configuration
# -------------------------
spark = SparkSession.builder \
    .appName("Assignment11_Final") \
    .master("local[4]") \
    .config("spark.driver.memory", "6g") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

# -------------------------
# Load books
# -------------------------
books_df = spark.read.text("hdfs://localhost:9000/D184MB/*.txt") \
    .withColumn("file_name", input_file_name())

# -------------------------
# Tokenize per line
# -------------------------
tokenizer = Tokenizer(inputCol="value", outputCol="words")
words_df = tokenizer.transform(books_df)

# -------------------------
# Remove stopwords
# -------------------------
remover = StopWordsRemover(inputCol="words", outputCol="filtered")
filtered_df = remover.transform(words_df)

# -------------------------
# Aggregate tokens per book
# -------------------------
books_tokens = filtered_df.groupBy("file_name") \
    .agg(flatten(collect_list("filtered")).alias("all_tokens"))

# -------------------------
# TF
# -------------------------
hashingTF = HashingTF(
    inputCol="all_tokens",
    outputCol="rawFeatures",
    numFeatures=500   # Reduced further for 8GB safety
)
featurized_df = hashingTF.transform(books_tokens)

# -------------------------
# IDF
# -------------------------
idf = IDF(inputCol="rawFeatures", outputCol="tfidf")
idfModel = idf.fit(featurized_df)
tfidf_df = idfModel.transform(featurized_df)

# -------------------------
# Normalize
# -------------------------
normalizer = Normalizer(inputCol="tfidf", outputCol="normFeatures")
normalized_df = normalizer.transform(tfidf_df) \
    .select("file_name", "normFeatures")

# Convert vectors to arrays
array_df = normalized_df \
    .withColumn("features_array", vector_to_array("normFeatures")) \
    .select("file_name", "features_array")

# -------------------------
# Pairwise Cosine Similarity
# -------------------------
df1 = array_df.alias("a")
df2 = array_df.alias("b")

similarity_df = df1.join(df2, col("a.file_name") < col("b.file_name")) \
    .withColumn(
        "cosine_similarity",
        expr("""
            aggregate(
                zip_with(a.features_array, b.features_array, (x, y) -> x * y),
                0D,
                (acc, x) -> acc + x
            )
        """)
    ) \
    .select(
        col("a.file_name").alias("book1"),
        col("b.file_name").alias("book2"),
        "cosine_similarity"
    ) \
    .orderBy(col("cosine_similarity").desc())

# -------------------------
# Show Top 5
# -------------------------
print("\nTop 5 Most Similar Book Pairs:")
similarity_df.show(5, truncate=False)

spark.stop()
