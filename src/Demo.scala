import org.apache.spark.sql.{SparkSession, functions => F}
import org.apache.spark.sql.expressions.Window

object Demo {
  def main(args: Array[String]): Unit = {
    // Initialize Spark session
    val spark = SparkSession.builder()
      .appName("Spark Scala Demo")
      .master("local[*]")
      .getOrCreate()

    // Load CSV data into DataFrame
    val df = spark.read.option("header", "true").option("inferSchema", "true").csv("data/sample_data.csv")

    // Filtering Data
    val filteredDf = df.filter(F.col("age") > 30)

    // Selecting Columns
    val selectedDf = df.select("name", "age")

    // Renaming Columns
    val renamedDf = df.withColumnRenamed("age", "years")

    // Using Alias for Columns
    val aliasedDf = df.select(F.col("age").alias("years"))

    // Creating new columns with alias
    val aliasedNewColDf = df.select(F.col("name"), F.col("age"), (F.col("age") + 10).alias("age_plus_10"))

    // Adding Columns
    val dfWithNewCol = df.withColumn("age_plus_10", F.col("age") + 10)

    // Dropping Columns
    val dfDroppedCol = df.drop("age")

    // Aggregating Data
    val avgAge = df.agg(F.avg("age")).first().getDouble(0)

    // Joining DataFrames
    val df2 = df.select("name", "salary")
    val joinedDf = df.join(df2, Seq("name"), "inner")

    // Handling Missing Values
    val dfDroppedNa = df.na.drop()
    val dfFilledNa = df.na.fill(Map("age" -> avgAge))

    // Sorting Data
    val sortedDf = df.orderBy(F.col("age").desc)

    // Window Functions
    val windowSpec = Window.partitionBy("department").orderBy(F.col("salary").desc)
    val dfWithRank = df.withColumn("rank", F.rank().over(windowSpec))

    // GroupBy and Aggregations
    val groupedDf = df.groupBy("department").agg(F.avg("salary").alias("avg_salary"))

    // Pivoting Data
    val pivotedDf = df.groupBy("department").pivot("gender").agg(F.avg("salary"))

    // Handling Dates and Timestamps
    val dfWithDate = df.withColumn("date", F.to_date(F.col("date_string"), "yyyy-MM-dd"))
    val dfWithDateParts = dfWithDate
      .withColumn("year", F.year(F.col("date")))
      .withColumn("month", F.month(F.col("date")))
      .withColumn("day", F.dayofmonth(F.col("date")))

    // User Defined Functions (UDFs)
    val toUppercase = F.udf((name: String) => name.toUpperCase)
    val dfWithUppercaseName = df.withColumn("name_uppercase", toUppercase(F.col("name")))

    // Show DataFrames
    aliasedDf.show()
    aliasedNewColDf.show()
    dfWithUppercaseName.show()

    // Stop Spark session
    spark.stop()
  }
}
