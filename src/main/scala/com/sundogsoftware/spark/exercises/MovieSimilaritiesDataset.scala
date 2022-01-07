package com.sundogsoftware.spark.exercises

import org.apache.spark.sql.functions._
import org.apache.log4j._

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructType}


object MovieSimilaritiesDataset {

  case class Movies(userID: Int, movieID: Int, rating: Int, timestamp: Long)
  case class MoviesNames(movieID: Int, movieTitle: String)
  case class MoviePairs(movie1: Int, movie2: Int, rating1: Int, rating2: Int)
  case class MoviePairsSimilarity(movie1: Int, movie2: Int, score: Double, numPairs: Long)
  case class MovieCooccurrences(movie1: Int, movie2: Int, cooccurs: BigInt)
  case class MovieOccurrences(movieID: Int, occurs: BigInt)

  def computeCosineSimilarity(spark: SparkSession, data: Dataset[MoviePairs]): Dataset[MoviePairsSimilarity] = {
    // Compute xx, xy and yy columns
    val pairScores = data
      .withColumn("xx", col("rating1") * col("rating1"))
      .withColumn("yy", col("rating2") * col("rating2"))
      .withColumn("xy", col("rating1") * col("rating2"))

    // Compute numerator, denominator and numPairs columns
    val calculateSimilarity = pairScores
      .groupBy("movie1", "movie2")
      .agg(
        sum(col("xy")).alias("numerator"),
        (sqrt(sum(col("xx"))) * sqrt(sum(col("yy")))).alias("denominator"),
        count(col("xy")).alias("numPairs")
      )

    // Calculate score and select only needed columns (movie1, movie2, score, numPairs)
    import spark.implicits._
    val result = calculateSimilarity
      .withColumn("score",
        when(col("denominator") =!= 0, col("numerator") / col("denominator"))
          .otherwise(null)
      ).select("movie1", "movie2", "score", "numPairs").as[MoviePairsSimilarity]

    result
  }

  def computePearsonCoefficient(spark: SparkSession, data: Dataset[MoviePairs]): Dataset[MoviePairsSimilarity] = {
    // Compute xx, xy and yy columns
    val pairScores = data
      .withColumn("xx", col("rating1") * col("rating1"))
      .withColumn("yy", col("rating2") * col("rating2"))
      .withColumn("xy", col("rating1") * col("rating2"))

    // Compute numerator, denominator and numPairs columns
    val calculateSimilarity = pairScores
      .groupBy("movie1", "movie2")
      .agg(
        (count(col("xy")) * sum(col("xy")) - sum(col("rating1")) * sum(col("rating2"))).alias("numerator"),
        sqrt(
          (count(col("xy")) * sum(col("xx")) - pow(sum(col("rating1")), 2)) *
            (count(col("xy")) * sum(col("yy")) - pow(sum(col("rating2")), 2))
        ).alias("denominator"),
        count(col("xy")).alias("numPairs")
      )

    calculateSimilarity.filter(row => row(0) == 196 || row(1) == 196).show(100)

    // Calculate score and select only needed columns (movie1, movie2, score, numPairs)
    import spark.implicits._
    val result = calculateSimilarity
      .withColumn("score",
        when(col("denominator") =!= 0, col("numerator") / col("denominator"))
          .otherwise(null)
      ).select("movie1", "movie2", "score", "numPairs").as[MoviePairsSimilarity]

    result
  }

  def computeJaccardCoefficient(spark: SparkSession, data: Dataset[Movies]): Dataset[MoviePairsSimilarity] = {

    val ratings = data.select("userId", "movieId", "rating")

    import spark.implicits._
    // Denominator in Jaccard
    val coocurrencesTemp = ratings.as("ratings1")
      .join(ratings.as("ratings2"), $"ratings1.userId" === $"ratings2.userId" && $"ratings1.movieId" < $"ratings2.movieId")
      .withColumn("movie1", $"ratings1.movieId")
      .withColumn("movie2", $"ratings2.movieId")
      .select("movie1", "movie2")

    coocurrencesTemp.show()

    val coocurrences = coocurrencesTemp
      .groupBy("movie1", "movie2")
      .agg(count("movie1"))
      .withColumnRenamed("count(movie1)", "cooccurs")
      .as[MovieCooccurrences]

    coocurrences.show()

    val occurrences = ratings.groupBy("movieId")
      .agg(count("movieId"))
      .withColumnRenamed("count(movieId)", "occurs")
      .as[MovieOccurrences]

    occurrences.show()

    val result = coocurrences
      .join(occurrences.as("movieOcc1"), $"movieOcc1.movieId" === $"movie1")
      .withColumnRenamed("occurs", "occurs1")
      .join(occurrences.as("movieOcc2"), $"movieOcc2.movieId" === $"movie2")
      .withColumnRenamed("occurs", "occurs2")
      .withColumn("score",
        when(col("occurs1") =!= 0, col("cooccurs") / (col("occurs1") + col("occurs2") - col("cooccurs")))
          .otherwise(0)
      ).select("movie1", "movie2", "score", "cooccurs")
      .withColumnRenamed("cooccurs", "numPairs")
      .as[MoviePairsSimilarity]

    result
  }

  def computeConditionalProbability(spark: SparkSession, data: Dataset[Movies], movieId: Int): Dataset[MoviePairsSimilarity] = {

    val ratings = data.select("userId", "movieId", "rating")

    import spark.implicits._
    // Denominator in Jaccard
    val coocurrencesTemp = ratings.as("ratings1")
      .join(ratings.as("ratings2"), $"ratings1.userId" === $"ratings2.userId" && $"ratings1.movieId" < $"ratings2.movieId")
      .withColumn("movie1", $"ratings1.movieId")
      .withColumn("movie2", $"ratings2.movieId")
      .select("movie1", "movie2")

    val coocurrences = coocurrencesTemp
      .groupBy("movie1", "movie2")
      .agg(count("movie1"))
      .withColumnRenamed("count(movie1)", "cooccurs")
      .as[MovieCooccurrences]

    val occurrences = ratings.groupBy("movieId")
      .agg(count("movieId"))
      .withColumnRenamed("count(movieId)", "occurs")
      .as[MovieOccurrences]

    val result = coocurrences
      .join(occurrences.as("movieOcc1"), $"movieOcc1.movieId" === $"movie1")
      .withColumnRenamed("occurs", "occurs1")
      .join(occurrences.as("movieOcc2"), $"movieOcc2.movieId" === $"movie2")
      .withColumnRenamed("occurs", "occurs2")
      .withColumn("score",
        when(col("movie1") =!= movieId, col("cooccurs") / col("occurs1"))
          .otherwise(col("cooccurs") / col("occurs2"))
      ).select("movie1", "movie2", "score", "cooccurs")
      .withColumnRenamed("cooccurs", "numPairs")
      .as[MoviePairsSimilarity]

    result
  }

  /** Get movie name by given movie id */
  def getMovieName(movieNames: Dataset[MoviesNames], movieId: Int): String = {
    val result = movieNames.filter(col("movieID") === movieId)
      .select("movieTitle").collect()(0)

    result(0).toString
  }
  /** Our main function where the action happens */
  def main(args: Array[String]) {

    val scoreThreshold = 0.0
    val coOccurrenceThreshold = 150.0

    val movieID: Int = args(0).toInt

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkSession using every core of the local machine
    val spark = SparkSession
      .builder
      .appName("MovieSimilarities")
      .master("local[*]")
      .getOrCreate()

    // Create schema when reading u.item
    val moviesNamesSchema = new StructType()
      .add("movieID", IntegerType, nullable = true)
      .add("movieTitle", StringType, nullable = true)

    // Create schema when reading u.data
    val moviesSchema = new StructType()
      .add("userID", IntegerType, nullable = true)
      .add("movieID", IntegerType, nullable = true)
      .add("rating", IntegerType, nullable = true)
      .add("timestamp", LongType, nullable = true)

    println("\nLoading movie names...")
    import spark.implicits._
    // Create a broadcast dataset of movieID and movieTitle.
    // Apply ISO-885901 charset
    val movieNames = spark.read
      .option("sep", "|")
      .option("charset", "ISO-8859-1")
      .schema(moviesNamesSchema)
      .csv("data/ml-100k/u.item")
      .as[MoviesNames]

    // Load up movie data as dataset
    val movies = spark.read
      .option("sep", "\t")
      .schema(moviesSchema)
      .csv("data/ml-100k/u.data")
      .as[Movies]

    val ratings = movies.select("userId", "movieId", "rating")

    // Emit every movie rated together by the same user.
    // Self-join to find every combination.
    // Select movie pairs and rating pairs
    val moviePairs = ratings.as("ratings1")
      .join(ratings.as("ratings2"), $"ratings1.userId" === $"ratings2.userId" && $"ratings1.movieId" < $"ratings2.movieId")
      .select($"ratings1.movieId".alias("movie1"),
        $"ratings2.movieId".alias("movie2"),
        $"ratings1.rating".alias("rating1"),
        $"ratings2.rating".alias("rating2")
      ).as[MoviePairs]

    //    val moviePairSimilarities = computeCosineSimilarity(spark, moviePairs).cache()
    //    val moviePairSimilarities = computePearsonCoefficient(spark, moviePairs).cache()
    //    val moviePairSimilarities = computeJaccardCoefficient(spark, movies).cache()
    val moviePairSimilarities = computeConditionalProbability(spark, movies, movieID).cache()

    // Filter for movies with this sim that are "good" as defined by
    // our quality thresholds above
    val filteredResults = moviePairSimilarities.filter(
      (col("movie1") === movieID || col("movie2") === movieID) &&
        col("score") > scoreThreshold && col("numPairs") > coOccurrenceThreshold)

    // Sort by quality score.
    val results = filteredResults.sort(col("score").desc).take(10)

    println("\nTop 10 similar movies for " + getMovieName(movieNames, movieID))
    for (result <- results) {
      // Display the similarity result that isn't the movie we're looking at
      var similarMovieID = result.movie1
      if (similarMovieID == movieID) {
        similarMovieID = result.movie2
      }
      println(getMovieName(movieNames, similarMovieID) + "\tscore: " + result.score + "\tstrength: " + result.numPairs)
    }
  }
}