import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{DateType, DoubleType, IntegerType, StringType, StructType, TimestampType}
import org.apache.spark.sql.functions.{avg, col, count, max, min, to_date, udf}

import java.sql.Date
import scala.util.{Failure, Success, Try}

object Main {

  def prepareDataForAdditionalTask(dataFrame: DataFrame): DataFrame = {
    def Round: Double => Long = { x => scala.math.round(x) }

    val round = udf(Round)
    val withRoundedDist = dataFrame.withColumn("Trip_distance_rounded", round(col("Trip_distance")))
    withRoundedDist.groupBy("Trip_distance_rounded").agg(avg("Tip_amount"), count("Tip_amount")).toDF("dist", "tip", "trip_count")
      .filter(col("dist") > 0 && col("trip_count") >= 20).sort("dist").drop(col("trip_count"))
  }

  def createParquet(spark: SparkSession, dataSetTemplate: String): Unit = {
    val schema = new StructType()
      .add("VendorID", IntegerType, nullable = true)
      .add("tpep_pickup_datetime", TimestampType, nullable = true)
      .add("tpep_dropoff_datetime", TimestampType, nullable = true)
      .add("passenger_count", IntegerType, nullable = true)
      .add("trip_distance", DoubleType, nullable = true)
      .add("RatecodeID", IntegerType, nullable = true)
      .add("store_and_fwd_flag", StringType, nullable = true)
      .add("PULocationID", IntegerType, nullable = true)
      .add("DOLocationID", IntegerType, nullable = true)
      .add("payment_type", IntegerType, nullable = true)
      .add("fare_amount", DoubleType, nullable = true)
      .add("extra", DoubleType, nullable = true)
      .add("mta_tax", DoubleType, nullable = true)
      .add("tip_amount", DoubleType, nullable = true)
      .add("tolls_amount", DoubleType, nullable = true)
      .add("improvement_surcharge", DoubleType, nullable = true)
      .add("total_amount", DoubleType, nullable = true)
      .add("congestion_surcharge", DoubleType, nullable = true)
    val df = spark.read.format("csv").option("header", "true").schema(schema).load(s"../$dataSetTemplate.csv")
    df.write.mode("overwrite").parquet(s"../$dataSetTemplate.parquet")
  }

  def loadDataFrame(spark: SparkSession, dataSetTemplate: String): DataFrame = {
    val res = Try(spark.read.parquet(s"../$dataSetTemplate.parquet"))
    res match {
      case Success(answer) => return answer
      case Failure(_) => createParquet(spark, dataSetTemplate)
    }
    spark.read.parquet(s"../$dataSetTemplate.parquet")
  }

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setAppName("Task_3.3")
      .setMaster("local")
    val sc = new SparkContext(sparkConf)
    val spark = SparkSession.builder.appName("Task 3.3").getOrCreate()

    // загружаем датасет, при необходимости конвертируем в parquet
    val df = loadDataFrame(spark, "yellow_tripdata_2020-01")
    // val df = loadDataFrame(spark, "small")

    // сохраняем данные для дополнительноно задания
    prepareDataForAdditionalTask(df).write.options(
      Map("header" -> "true", "delimiter" -> ",")).mode("overwrite").csv("../dist_and_trip.csv")

    // добавлям столбец с датой поездки (без времени, что бы было удобно обрабатывать)
    val withDate = df.withColumn("pickup_date", to_date(col("tpep_pickup_datetime")))

    // достаём данные по всем 4м группам

    def getGroupData(column: Column): DataFrame = {
      withDate.filter(column).groupBy(col("pickup_date"))
        .agg(min("total_amount"), max("total_amount"), count("total_amount"))
        .toDF("date", "cheapest", "dearest", "total_trip")
    }

    val p0 = getGroupData(withDate("passenger_count") === 0)
    val p1 = getGroupData(withDate("passenger_count") === 1)
    val p2 = getGroupData(withDate("passenger_count") === 2)
    val p3 = getGroupData(withDate("passenger_count") === 3)
    val p4 = getGroupData(withDate("passenger_count") > 3)

    // достаём все даты поездок
    val dates = withDate.select("pickup_date")
      .dropDuplicates()

    // схема выходной таблицы
    val parquetSchema = new StructType()
      .add("date", DateType)
      .add("percentage_zero", DoubleType)
      .add("cheapest_zero", DoubleType)
      .add("dearest_zero", DoubleType)
      .add("percentage_1p", DoubleType)
      .add("cheapest_1p", DoubleType)
      .add("dearest_1p", DoubleType)
      .add("percentage_2p", DoubleType)
      .add("cheapest_2p", DoubleType)
      .add("dearest_2p", DoubleType)
      .add("percentage_3p", DoubleType)
      .add("cheapest_3p", DoubleType)
      .add("dearest_3p", DoubleType)
      .add("percentage_4plus", DoubleType)
      .add("cheapest_4plus", DoubleType)
      .add("dearest_4plus", DoubleType)

    // создаём пустую таблицу
    var parquetDf = spark.createDataFrame(sc.emptyRDD[Row], parquetSchema)

    // пробегаемся по всем датам и наполняем выходную таблицу
    for (elem <- dates.collect()) {
      val date = elem.getAs[Date](0)

      // функция-помощник для получения общих данных для каждой группы пассажиров
      def getCommonData(df: DataFrame): (Double, Double, Long) = {
        val dataFrame = df.filter(col("date") === date)
        if (dataFrame.count() == 0) {
          (0, 0, 0)
        } else {
          val first = dataFrame.first()
          (first.getAs[Double](1), first.getAs[Double](2), first.getAs[Long](3))
        }
      }

      val (p0min, p0max, p0Total) = getCommonData(p0)
      val (p1min, p1max, p1Total) = getCommonData(p1)
      val (p2min, p2max, p2Total) = getCommonData(p2)
      val (p3min, p3max, p3Total) = getCommonData(p3)
      val (p4min, p4max, p4Total) = getCommonData(p4)
      val total = p0Total + p1Total + p2Total + p3Total + p4Total
      val onePercent = total.toDouble / 100

      def correct(value: Double): Double = {
        if (value == 0) 0 else value / onePercent
      }

      val p0Percent = correct(p0Total)
      val p1Percent = correct(p1Total)
      val p2Percent = correct(p2Total)
      val p3Percent = correct(p3Total)
      val p4Percent = correct(p4Total)
      parquetDf = parquetDf.union(
        spark.createDataFrame(Seq(
          (date,
            p0Percent, p0min, p0max,
            p1Percent, p1min, p1max,
            p2Percent, p2min, p2max,
            p3Percent, p3min, p3max,
            p4Percent, p4min, p4max)
        )))
    }

    // пишем таблицу на диск
    parquetDf.write.mode("overwrite").parquet("../result.parquet")
    println("result len: 0" + parquetDf.count())
  }
}
