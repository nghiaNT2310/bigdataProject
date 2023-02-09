import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import functions as F
spark = SparkSession \
    .builder \
    .appName("Spotify") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.2") \
    .getOrCreate()

album_tbl = spark.read\
    .format('com.mongodb.spark.sql.DefaultSource')\
    .option("uri", "mongodb+srv://bkfashionshop:project1@bigdata.mncihbp.mongodb.net/test.Albums") \
    .load()

track_tbl = spark.read\
    .format('com.mongodb.spark.sql.DefaultSource')\
    .option("uri", "mongodb+srv://bkfashionshop:project1@bigdata.mncihbp.mongodb.net/test.Tracks") \
    .load()


df2 = album_tbl.select(to_date(col("release_date")).alias("release_date"))

numberReleaseByYearDF = df2.groupBy(
    year("release_date").alias("year")).count().sort("year")

numberReleaseByYearDF.write.format("com.mongodb.spark.sql.DefaultSource")\
    .mode("append")\
    .option("spark.mongodb.output.uri", "mongodb+srv://bkfashionshop:project1@bigdata.mncihbp.mongodb.net/test.NumberAlbumReleaseByYear")\
    .save()

numberReleaseByMonthDF = df2.groupBy(
    month("release_date").alias("month")).count().sort("month")

numberReleaseByMonthDF.write.format("com.mongodb.spark.sql.DefaultSource")\
    .mode("append")\
    .option("spark.mongodb.output.uri", "mongodb+srv://bkfashionshop:project1@bigdata.mncihbp.mongodb.net/test.NumberAlbumReleaseByMonth")\
    .save()

numberReleaseByMonthAdnYearDF = df2.groupBy(
    [year("release_date").alias("year"), month("release_date").alias("month")]).count().sort(["year", "month"])

numberReleaseByMonthAdnYearDF.write.format("com.mongodb.spark.sql.DefaultSource")\
    .mode("append")\
    .option("spark.mongodb.output.uri", "mongodb+srv://bkfashionshop:project1@bigdata.mncihbp.mongodb.net/test.NumberAlbumReleaseByYearAndMonth")\
    .save()


# tracks_df = track_tbl.filter("release_year is not null")\
#     .select(col("release_year").alias("release_year"), col("feature").alias("feature"))\
#     .groupBy("release_year")\
#     .agg(F.mean("feature.danceability").alias("danceability"), F.mean("feature.energy").alias("energy"), F.mean("feature.acousticness").alias("acousticness"), F.mean("feature.instrumentalness").alias("instrumentalness"), F.mean("feature.liveness").alias("liveness"), F.mean("feature.speechiness").alias("speechiness"), F.mean("feature.speechiness").alias("speechiness"))\
#     .show(n=10)

# tracks_df.write.format("com.mongodb.spark.sql.DefaultSource")\
#     .mode("append")\
#     .option("spark.mongodb.output.uri", "mongodb+srv://nghiango:nghiango23102001@cluster0.pjnmw.mongodb.net/BIGDATA.FeatureTrack")\
#     .save()
