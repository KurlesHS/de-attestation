import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DateType, DoubleType, IntegerType, StringType, StructType, TimestampType}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.{col, count, max, min, avg, to_date, udf}

import java.sql.Date
import scala.util.{Try, Success, Failure}
object Main {

  def prepareDataForAdditionalTask(dataFrame: DataFrame): DataFrame = {
    def Round : Double => Long = { x => scala.math.round(x)}
    val round = udf(Round)
    val withRoundedDist = dataFrame.withColumn("Trip_distance_rounded", round(col("Trip_distance")))
    withRoundedDist.groupBy("Trip_distance_rounded").agg(avg("Tip_amount"), count("Tip_amount")).toDF("dist", "tip", "trip_count")
      .filter(col("dist") > 0 && col("trip_count") >= 20).sort("dist").drop(col("trip_count"))
  }

  def createParquet(spark: SparkSession, dataSetTemplate : String): Unit = {
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

  def loadDataFrame(spark: SparkSession, dataSetTemplate : String) : DataFrame = {
    val res  = Try(spark.read.parquet(s"../$dataSetTemplate.parquet"))
    res match {
      case Success(answer) => return answer
      case Failure(_) => createParquet(spark, dataSetTemplate)
    }
    spark.read.parquet(s"../$dataSetTemplate.parquet")
  }

  // 23:02:42
  // 23:07:41

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setAppName("Task_3.3")
      .setMaster("local")
    val sc = new SparkContext(sparkConf)
    val spark = SparkSession.builder.appName("Task 3.3").getOrCreate()

    // val df = loadDataFrame(spark, "small")

    // загружаем датасет, при необходимости конвертируем в parquet
    val df = loadDataFrame(spark, "yellow_tripdata_2020-01")

    // сохраняем данные для дополнительноно задания
    prepareDataForAdditionalTask(df).write.options(
      Map("header"->"true", "delimiter"->",")).mode("overwrite").csv("../dist_and_trip.csv")

    // добавлям столбец с датой поездки (без времени, что бы было удобно обрабатывать)
    val withDate = df.withColumn("pickup_date", to_date(col("tpep_pickup_datetime")))

    // достаём данные по всем 4м группам
    val p0 = withDate.filter(withDate("passenger_count") === 0).groupBy(col("pickup_date"))
      .agg(min("total_amount"), max("total_amount"), count("total_amount"))
      .toDF("date", "cheapest", "dearest", "total_trip")

    val p1 = withDate.filter(withDate("passenger_count") === 1).groupBy(col("pickup_date"))
      .agg(min("total_amount"), max("total_amount"), count("total_amount"))
      .toDF("date", "cheapest", "dearest", "total_trip")

    val p2 = withDate.filter(withDate("passenger_count") === 2).groupBy(col("pickup_date"))
      .agg(min("total_amount"), max("total_amount"), count("total_amount"))
      .toDF("date", "cheapest", "dearest", "total_trip")

    val p3 = withDate.filter(withDate("passenger_count") === 3).groupBy(col("pickup_date"))
      .agg(min("total_amount"), max("total_amount"), count("total_amount"))
      .toDF("date", "cheapest", "dearest", "total_trip")

    val p4 = withDate.filter(withDate("passenger_count") > 3).groupBy(col("pickup_date"))
      .agg(min("total_amount"), max("total_amount"), count("total_amount"))
      .toDF("date", "cheapest", "dearest", "total_trip")

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

    // функция-помощник для получения общих данныд для каждой группы пассажиров
    def helper(dataFrame: DataFrame) :  (Double, Double, Long) = {
      if (dataFrame.count() == 0) {
         (0, 0, 0)
      } else {
        val first = dataFrame.first()
         (first.getAs[Double](1), first.getAs[Double](2), first.getAs[Long](3))
      }
    }

    // пробегаемся по всем датам и создаём выходную таблицу
    for (elem <- dates.collect()) {
      val date = elem.getAs[Date](0)
      val p0d = p0.filter(col("date") === date)
      val p1d = p1.filter(col("date") === date)
      val p2d = p2.filter(col("date") === date)
      val p3d = p3.filter(col("date") === date)
      val p4d = p4.filter(col("date") === date)

      val (p0min, p0max, p0Total) = helper(p0d)
      val (p1min, p1max, p1Total) = helper(p1d)
      val (p2min, p2max, p2Total) = helper(p2d)
      val (p3min, p3max, p3Total) = helper(p3d)
      val (p4min, p4max, p4Total) = helper(p4d)
      val total = p0Total + p1Total + p2Total + p3Total + p4Total
      val onePercent = total.toDouble / 100
      def helper2(value: Double): Double = {if (value ==0 ) 0 else value / onePercent}
      val p0Percent = helper2(p0Total)
      val p1Percent = helper2(p1Total)
      val p2Percent = helper2(p2Total)
      val p3Percent = helper2(p3Total)
      val p4Percent = helper2(p4Total)
      val tmpSeq = Seq(
        (date,
          p0Percent, p0min, p0max,
          p1Percent, p1min, p1max,
          p2Percent, p2min, p2max,
          p3Percent, p3min, p3max,
          p4Percent, p4min, p4max)
      )
      val tmpDf = spark.createDataFrame(tmpSeq)
      parquetDf = parquetDf.union(tmpDf)
    }

    // пишем таблицу на диск
    parquetDf.write.mode("overwrite").parquet("../result.parquet")
  }
}
