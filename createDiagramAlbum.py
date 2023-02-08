import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import *
spark = SparkSession \
    .builder \
    .appName("Spotify") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.2") \
    .config("spark.mongodb.input.uri", "mongodb+srv://bkfashionshop:project1@bigdata.mncihbp.mongodb.net/test.Albums")\
    .getOrCreate()

# books_tbl = spark.read\
#     .format('com.mongodb.spark.sql.DefaultSource')\
#     .option("uri", "mongodb+srv://bkfashionshop:project1@bigdata.mncihbp.mongodb.net/test.Albums") \
#     .load()


# df2 = books_tbl.select(to_date(col("release_date")).alias("release_date"))

# numberReleaseByYearDF = df2.groupBy(
#     year("release_date").alias("year")).count().sort("year")

# numberReleaseByYearDF.write.format("com.mongodb.spark.sql.DefaultSource")\
#     .mode("append")\
#     .option("spark.mongodb.output.uri", "mongodb+srv://bkfashionshop:project1@bigdata.mncihbp.mongodb.net/test.NumberAlbumReleaseByYear")\
#     .save()

# numberReleaseByMonthDF = df2.groupBy(
#     month("release_date").alias("month")).count().sort("month")

# numberReleaseByMonthDF.write.format("com.mongodb.spark.sql.DefaultSource")\
#     .mode("append")\
#     .option("spark.mongodb.output.uri", "mongodb+srv://bkfashionshop:project1@bigdata.mncihbp.mongodb.net/test.NumberAlbumReleaseByMonth")\
#     .save()

# numberReleaseByMonthAdnYearDF = df2.groupBy(
#     [year("release_date").alias("year"), month("release_date").alias("month")]).count().sort(["year", "month"])

# numberReleaseByMonthAdnYearDF.write.format("com.mongodb.spark.sql.DefaultSource")\
#     .mode("append")\
#     .option("spark.mongodb.output.uri", "mongodb+srv://bkfashionshop:project1@bigdata.mncihbp.mongodb.net/test.NumberAlbumReleaseByYearAndMonth")\
#     .save()
