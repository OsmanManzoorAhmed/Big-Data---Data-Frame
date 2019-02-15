package questions

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{to_date, to_timestamp, concat_ws}

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions

import org.apache.spark.sql._

import org.apache.spark.sql.expressions.Window
import org.apache.zookeeper.Op.Create
import org.spark_project.guava.collect.Table
/** AirTrafficProcessor provides functionalites to
* process air traffic data
* Spark SQL tables available from start:
*   - 'carriers' airliner information
*   - 'airports' airport information
*
* After implementing the first method 'loadData'
* table 'airtraffic',which provides flight information,
* is also available. You can find the raw data from /resources folder.
* Useful links:
* http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.Dataset
* http://spark.apache.org/docs/latest/sql-programming-guide.html
* https://github.com/databricks/spark-csv
*
* We are using data from http://stat-computing.org/dataexpo/2009/the-data.html
*
* @param spark reference to SparkSession
* @param filePath path to the air traffic data
* @param airportsPath path to the airport data
* @param carriersPath path to the carrier data
*/
class AirTrafficProcessor(spark: SparkSession,
    filePath:String, airportsPath: String, carriersPath: String) {

    import spark.implicits._

    /*
    * Table names for SQL
    * use airTraffic for table name in loadData
    * method.
    */
    val airTraffic = "airtraffic"
    val carriers = "carriers"
    val airports = "airports"

    // load the files and create tables
    // for spark sql
    //DO NOT EDIT
    val carriersTable = spark.read
        .option("header","true")
        .option("inferSchema", "true")
        .csv(carriersPath)
    carriersTable.createOrReplaceTempView(carriers)
    //DO NOT EDIT
    val airportsTable = spark.read
        .option("header","true")
        .option("inferSchema", "true")
        .csv(airportsPath)
    airportsTable.createOrReplaceTempView(airports)

    /** load the data and register it as a table
    * so that we can use it later for spark SQL.
    * File is in csv format, so we are using
    * Spark csv library.
    *
    * Use the variable airTraffic for table name.
    *
    * Example on how the csv library should be used can be found:
    * https://github.com/databricks/spark-csv
    *
    * Note:
    *   If you just load the data using 'inferSchema' -> 'true'
    *   some of the fields which should be Integers are casted to
    *   Strings. That happens, because NULL values are represented
    *   as 'NA' strings in this data.
    *
    *   E.g:
    *   2008,7,2,3,733,735,858,852,DL,1551,N957DL,85,77,42,6,-2,CAE,
    *   ATL,191,15,28,0,,0,NA,NA,NA,NA,NA
    *
    *   Therefore you should remove 'NA' strings and replace them with
    *   NULL. Option 'nullValue' in csv library is useful. However you
    *   don't need to take empty Strings into account.
    *   For instance TailNum field contains empty strings.
    *
    * Correct Schema:
    *   |-- Year: integer (nullable = true)
    *   |-- Month: integer (nullable = true)
    *   |-- DayofMonth: integer (nullable = true)
    *   |-- DayOfWeek: integer (nullable = true)
    *   |-- DepTime: integer (nullable = true)
    *   |-- CRSDepTime: integer (nullable = true)
    *   |-- ArrTime: integer (nullable = true)
    *   |-- CRSArrTime: integer (nullable = true)
    *   |-- UniqueCarrier: string (nullable = true)
    *   |-- FlightNum: integer (nullable = true)
    *   |-- TailNum: string (nullable = true)
    *   |-- ActualElapsedTime: integer (nullable = true)
    *   |-- CRSElapsedTime: integer (nullable = true)
    *   |-- AirTime: integer (nullable = true)
    *   |-- ArrDelay: integer (nullable = true)
    *   |-- DepDelay: integer (nullable = true)
    *   |-- Origin: string (nullable = true)
    *   |-- Dest: string (nullable = true)
    *   |-- Distance: integer (nullable = true)
    *   |-- TaxiIn: integer (nullable = true)
    *   |-- TaxiOut: integer (nullable = true)
    *   |-- Cancelled: integer (nullable = true)
    *   |-- CancellationCode: string (nullable = true)
    *   |-- Diverted: integer (nullable = true)
    *   |-- CarrierDelay: integer (nullable = true)
    *   |-- WeatherDelay: integer (nullable = true)
    *   |-- NASDelay: integer (nullable = true)
    *   |-- SecurityDelay: integer (nullable = true)
    *   |-- LateAircraftDelay: integer (nullable = true)
    *
    * @param path absolute path to the csv file.
    * @return created DataFrame with correct column types.
    */
    def loadDataAndRegister(path: String): DataFrame = {

        val airTrafficTable = spark.read
          .format("com.databricks.spark.csv")
          .option("header", "true") // Use first line of all files as header
          .option("nullValue","NA")
          .option("inferSchema", "true") // Automatically infer data types
          .load(path)

        airTrafficTable.createOrReplaceTempView(airTraffic)

        return airTrafficTable
    }

    //USE can use SPARK SQL or DataFrame transformations

    /** Gets the number of flights for each
    * airplane. 'TailNum' column is unique for each
    * airplane so it should be used. DataFrame should
    * also be sorted by count in descending order.
    *
    * Result looks like:
    *   +-------+-----+
    *   |TailNum|count|
    *   +-------+-----+
    *   | N635SW| 2305|
    *   | N11150| 1342|
    *   | N572UA| 1176|
    *   | N121UA|    8|
    *   +-------+-----+
    *
    * @param df Air traffic data
    * @return DataFrame containing number of flights per
    * TailNum. DataFrame is sorted by count. Column names
    * are TailNum and count
    */
    def flightCount(df: DataFrame): DataFrame = {

        val numFlights = df.sqlContext.sql("SELECT TailNum, COUNT(TailNum) as count FROM airTraffic group by TailNum order by count DESC")

        return numFlights
    }


    /** Which flights were cancelled due to
    * security reasons the most?
    *
    * Example output:
    * +---------+----+
    * |FlightNum|Dest|
    * +---------+----+
    * |     4285| DHN|
    * |     4790| ATL|
    * |     3631| LEX|
    * |     3632| DFW|
    * +---------+----+
    *
    * @return Returns a DataFrame containing flights which were
    * cancelled due to security reasons (CancellationCode = 'D').
    * Columns FlightNum and Dest are included.
    */
    def cancelledDueToSecurity(df: DataFrame): DataFrame = {
        val cancelledFlights = df.sqlContext.sql("""SELECT FlightNum,Dest FROM airTraffic where CancellationCode = 'D'""")
        return cancelledFlights
    }

    /** What was the longest weather delay between January
    * and march (1.1-31.3)?
    *
    * Example output:
    * +----+
    * | _c0|
    * +----+
    * |1148|
    * +----+
    *
    * @return DataFrame containing the highest delay which
    * was due to weather.
    */
    def longestWeatherDelay(df: DataFrame): DataFrame = {
        val longestDelay = df.sqlContext.sql("""SELECT MAX(WeatherDelay) FROM airTraffic where Month between 1 and 3""")
        return longestDelay
    }

    /** Which airliners didn't fly.
    * Table 'carriers' has this information.
    *
    * Example output:
    * +--------------------+
    * |         Description|
    * +--------------------+
    * |      Air Comet S.A.|
    * |   Aerodynamics Inc.|
    * |  Combs Airways Inc.|
    * |   Evanston Aviation|
    * |Lufthansa Cargo A...|
    * +--------------------+
    *
    * @return airliner descriptions
    */
    def didNotFly(df: DataFrame): DataFrame = {
        val didNotFly = df.sqlContext.sql("""SELECT Description FROM carriers LEFT OUTER JOIN airTraffic ON carriers.Code == airTraffic.UniqueCarrier  where airTraffic.UniqueCarrier IS NULL""")

        return didNotFly
    }

    /** Find the airliners which travel
    * from Vegas to JFK. Sort them in descending
    * order by number of flights and append the
    * DataFrame with the data from carriers.csv.
    * Spark SQL table 'carriers' contains this data.
    * Vegas iasa code: LAS
    * JFK iasa code: JFK
    *
    *   Output should look like:
    *
    *   +--------------------+----+
    *   |         Description| Num|
    *   +--------------------+----+
    *   |     JetBlue Airways|1824|
    *   |Delta Air Lines Inc.|1343|
    *   |US Airways Inc. (...| 948|
    *   |American Airlines...| 366|
    *   +--------------------+----+
    *
    * @return DataFrame containing Columns Description
    * (airliner name) and Num(number of flights) sorted
    * in descending order.
    */
    def flightsFromVegasToJFK(df: DataFrame): DataFrame = {
        val flightsFromVegas = df.sqlContext.sql(
          """SELECT Description, count(*)
            |as Num FROM airTraffic  JOIN carriers ON carriers.Code == airTraffic.UniqueCarrier
            |where airTraffic.Origin == 'LAS' and airTraffic.Dest == 'JFK' group by carriers.Description order by Num DESC""".stripMargin)

        return flightsFromVegas
    }

    /** How much time airplanes spend on moving from
    * gate to the runway and vise versa at the airport on average.
    * This method should return a DataFrame containing this
    * information per airport.
    * Columns 'TaxiIn' and 'TaxiOut' tells time spend on taxiing.
    * Order by time spend on taxiing in ascending order. 'TaxiIn'
    * means time spend on taxiing in departure('Origin') airport
    * and 'TaxiOut' spend on taxiing in arrival('Dest') airport.
    * Column name's should be 'airport' for ISA airport code and
    * 'taxi' for average time spend taxiing
    *
    *
    * DataFrame should look like:
    *   +-------+-----------------+
    *   |airport|             taxi|
    *   +-------+-----------------+
    *   |    BRW|5.084736087380412|
    *   |    OME|5.961294471783976|
    *   |    OTZ|6.866595496262391|
    *   |    DAL|6.983973195822733|
    *   |    HRL|7.019248180512919|
    *   |    SCC|7.054629009320311|
    *   +-------+-----------------+
    *
    * @return DataFrame containing time spend on taxiing per
    * airport ordered in ascending order.
    */
    def timeSpentTaxiing(df: DataFrame): DataFrame = {
        //val timeTaxiing = df.sqlContext.sql(
        //  """SELECT airport, AVG(taxi) as taxi from
        //   |(SELECT Origin as airport, AVG(TaxiIn) as taxi from airTraffic group by airport order by taxi ASC
        //  |UNION SELECT Dest as airport, AVG(TaxiOut) as taxi from airTraffic group by airport order by taxi ASC)
        //    |group by airport order by taxi ASC""".stripMargin)

        val taxiIn = df.sqlContext.sql(""" select Origin as airport, AVG(TaxiIn) as taxii from airTraffic group by airport""")
        taxiIn.createOrReplaceTempView("taxiIN")
        val taxiOut = df.sqlContext.sql("""select Dest as airport, AVG(TaxiOut) as taxii from airTraffic group by airport""")
        taxiOut.createOrReplaceTempView("taxiOUT")

        val timeTaxi = df.sqlContext.sql(
          """ select taxiIn.airport, (taxiIN.taxii + taxiOUT.taxii) as taxi
            | from taxiIN join taxiOUT on taxiIN.airport == taxiOUT.airport
            | order by taxi ASC
          """.stripMargin)

        return timeTaxi
    }

    /** What is the median travel distance?
    * Field Distance contains this information.
    *
    * Example output:
    * +-----+
    * |  _c0|
    * +-----+
    * |581.0|
    * +-----+
    *
    * @return DataFrame containing the median value
    */
    def distanceMedian(df: DataFrame): DataFrame = {
        val distMed = df.sqlContext.sql("select percentile(Distance, 0.5) as _c0 from airTraffic")

         return distMed
    }

    /** What is the carrier delay, below which 95%
    * of the observations may be found?
    *
    * Example output:
    * +----+
    * | _c0|
    * +----+
    * |77.0|
    * +----+
    *
    * @return DataFrame containing the carrier delay
    */
    def score95(df: DataFrame): DataFrame = {
        val score = df.sqlContext.sql("select percentile(CarrierDelay, 0.95) as _c0 from airTraffic")

        return score
    }


    /** From which airport are flights cancelled the most?
    * What percentage of flights are cancelled from a specific
    * airport?
    * cancelledFlights combines flight data with
    * location data. Returns a DataFrame containing
    * columns 'airport' and 'city' from 'airports'
    * and the sum of number of cancelled flights divided
    * by the number of all flights (we get percentage)
    * from 'airtraffic' table. Name should be 'percentage'.
    * Lastly result should be ordered by 'percentage' and
    * secondly by 'airport' both in descending order.
    *
    *
    *
    *
    *   Output should look something like this:
    *   +--------------------+--------------------+--------------------+
    *   |             airport|                city|          percentage|
    *   +--------------------+--------------------+--------------------+
    *   |  Telluride Regional|           Telluride|   0.211340206185567|
    *   |  Waterloo Municipal|            Waterloo| 0.17027863777089783|
    *   |Houghton County M...|             Hancock| 0.12264150943396226|
    *   |                Adak|                Adak| 0.09803921568627451|
    *   |Aspen-Pitkin Co/S...|               Aspen| 0.09157716223855286|
    *   |      Sioux Gateway |          Sioux City| 0.09016393442622951|
    *   +--------------------+--------------------+--------------------+
    *
    * @return DataFrame containing columns airport, city and percentage
    */
    def cancelledFlights(df: DataFrame): DataFrame = {
        //val cancelledFlight = df.sqlContext.sql(
        //  """ select airport, city,
        //    |(air / (select airTotal from (select airport, city, count(*) as airTotal from airTraffic
        //    |join airports on airports.iata == airTraffic.origin
        //    |where airports.airport == airport and airports.city == city group by airports.airport, airports.city))) as percentage from
        //    |(select airport, city, count(*) as air from airTraffic
        //    |join airports on airports.iata == airTraffic.origin where Cancelled == 1
        //    |group by airport, city)
        //    |order by percentage desc, airport desc""".stripMargin)

        val cancelFlights = df.sqlContext.sql(
            """ select origin, count(*) as air from airTraffic
            | where Cancelled == 1 group by origin""".stripMargin)
        //val cancelF = "cancelF"
        cancelFlights.createOrReplaceTempView("cancelF")
        val totalFlights = df.sqlContext.sql(
          """ select origin, count(*) as air from airTraffic group by origin""".stripMargin)
        //val totalF = "totalF"
        totalFlights.createOrReplaceTempView("totalF")

        val cancelledInfo = spark.sql(
          """ select airport, city, (cancelF.air / totalF.air) as percentage
            |from cancelF join totalF on cancelF.origin == totalF.origin
            |join airports on airports.iata == cancelF.origin
            |order by percentage desc, airport desc
          """.stripMargin)

        return cancelledInfo
    }

    /**
    * Calculates the linear least squares approximation for relationship
    * between DepDelay and WeatherDelay.
    *  - First filter out entries where DepDelay < 0
    *  - there are definitely multiple data points for a single
    *    DepDelay value so calculate the average WeatherDelay per
    *    DepDelay
    *  - Calculate the linear least squares (c+bx=y) where
    *    x equals DepDelay and y WeatherDelay. c is the constant term
    *    and b is the slope.
    *
    *  
    * @return tuple, which has the constant term first and the slope second
    */
    def leastSquares(df: DataFrame):(Double, Double) = {

        val FilterDepDelay = df.sqlContext.sql("""select DepDelay, WeatherDelay from airtraffic where DepDelay >= 0""")
        FilterDepDelay.createOrReplaceTempView("FilterDepDelay")
        val averageDelay = df.sqlContext.sql("""select DepDelay, avg(WeatherDelay) as WeatherDelay from FilterDepDelay group by DepDelay""")
        averageDelay.createOrReplaceTempView("averageDelay")
        val N = df.sqlContext.sql("""select count(*) as c from averageDelay""").first().get(0).toString().toDouble

        val sumXY = df.sqlContext.sql("""select  sum(DepDelay * WeatherDelay) as sumxy from averageDelay""").first().get(0).toString().toDouble
        val sumX = df.sqlContext.sql("""select  sum(DepDelay) as sumx from averageDelay""").first().get(0).toString().toDouble
        val sumY = df.sqlContext.sql("""select  sum(WeatherDelay) as sumy from averageDelay""").first().get(0).toString().toDouble
        val sumxsquare = df.sqlContext.sql("""select  sum(DepDelay * DepDelay) as sumxsquare from averageDelay""").first().get(0).toString().toDouble
        val sumxwholesquare = sumX * sumX;

        val slope = ((N * sumXY )- (sumX * sumY))/((N * sumxsquare) - sumxwholesquare)
        val constant = (sumY - (slope * sumX)) / (N)


        return(constant,slope)
    }

    /**
    * Calculates the running average for DepDelay per day.
    * Average should be taken from both sides of the day we
    * are calculating the running average. You should take
    * 5 days from both sides of the day you are counting the running
    * average into consideration. For instance
    * if we want to calculate the average for the 10th of
    * January we have to take days: 5,6,7,8,9,10,11,12,13,14 and
    * 15 into account.
    *
    * Complete example
    * Let's assume that data looks like this
    *
    * +----+-----+---+--------+
    * |Year|Month|Day|DepDelay|
    * +----+-----+---+--------+
    * |2008|    3| 27|      12|
    * |2008|    3| 27|      -2|
    * |2008|    3| 28|       3|
    * |2008|    3| 29|      -5|
    * |2008|    3| 29|      12|
    * |2008|    3| 30|       5|
    * |2008|    3| 31|      47|
    * |2008|    4|  1|      45|
    * |2008|    4|  1|       2|
    * |2008|    4|  2|      -6|
    * |2008|    4|  3|       0|
    * |2008|    4|  3|       4|
    * |2008|    4|  3|      -2|
    * |2008|    4|  4|       2|
    * |2008|    4|  5|      27|
    * +----+-----+---+--------+
    *
    *
    * When running average is calculated
    * +----------+------------------+
    * |      date|    moving_average|
    * +----------+------------------+
    * |2008-03-27|13.222222222222221|
    * |2008-03-28|              11.3|
    * |2008-03-29| 8.846153846153847|
    * |2008-03-30| 8.357142857142858|
    * |2008-03-31|               9.6|
    * |2008-04-01|               9.6|
    * |2008-04-02|10.307692307692308|
    * |2008-04-03|10.916666666666666|
    * |2008-04-04|              12.4|
    * |2008-04-05|13.222222222222221|
    * +----------+------------------+
    *
    * For instance running_average for 
    * date 2008-04-03 => (-5+12+5+47+45+2-6+0+4-2+2+27)/12
    * =10.916666
    *
    *
    * @return DataFrame with schema 'date' (YYYY-MM-DD) (string type) and 'moving_average' (double type)
    * ordered by 'date'
    */
    def runningAverage(df: DataFrame): DataFrame = {

        //val  average = df.sqlContext.sql(
        //    """ select CAST(Year AS VARCHAR(4))
        //      | dat, DepDelay from airtraffic""".stripMargin)

        //val average = df.withColumn("merge", concat_ws("-", $"Year", $"Month", $"DayofMonth"))
        //  .withColumn("date", to_date($"merge", "yyyy-MM-dd"))
        //  .drop("merge")
        //average.createOrReplaceTempView("DateTable")

        val average = df.withColumn("merge", concat_ws("-", $"Year", $"Month", $"DayofMonth"))
          .withColumn("date", to_date($"merge", "yyyy-MM-dd"))
          .drop("merge")
        average.createOrReplaceTempView("DateTable")


        val averageTable = spark.sql(""" select date, DepDelay from DateTable""")
        averageTable.createOrReplaceTempView("averageData")
        averageTable.createOrReplaceTempView("averageTable")
        //val wSpec1 = Window.partitionBy("date").orderBy("date").rowsBetween(-1, 1)

        //val w = Window()
        //  .partitionBy(col(""))
        //  .orderBy(col("start").cast("timestamp").cast("long"))
        //  .rangeBetween(-days(7), 0)

        //val av = df.sqlContext.sql(
        //""" select t1.date, AVG(t2.DepDelay)
        //    | from averageTable as t1 join averageTable as t2 on t2.date between DATE_ADD(t1.date, -5) and DATE_ADD(t1.date,5)
        //    | group by t1.date
        //  """.stripMargin)


        //val wd = (Window.orderBy(F.col("timestampGMT").cast('long')).rangeBetween(-days(7), 0))
        val w = Window.orderBy(averageTable.col("date").cast("long")).rangeBetween(-5*86400,5*86400)
        val av = averageTable.withColumn("moving_average",avg("DepDelay").over(w)).select("date","moving_average")


        return av
    }
}
/**
*
*  Change the student id
*/
object AirTrafficProcessor {
    val studentId = "721347"
}
