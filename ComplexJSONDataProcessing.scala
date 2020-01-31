package com.bigdata.spark.sparksql

import org.apache.spark.sql.SparkSession

import org.apache.spark.sql._

object ComplexJSONDataProcessing {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("ComplexJSONDataProcessing").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("ComplexJSONDataProcessing").getOrCreate()
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
    import spark.implicits._
    import spark.sql
    val input = args(0)
    val output = args(1)
    val df = spark.read.format("json").option("inferSchema", "true").load(input)
    df.createOrReplaceTempView("tab")
    val query = "select `r`.`restaurant`.* from tab lateral view explode(restaurants) X as r"
    val res = spark.sql(query)
    res.createOrReplaceTempView("tab1")
    val query1 = spark.sql("select *, `o`.`offer`.*, `z`.`event`.* from tab1 lateral view explode(offers) t as o lateral view explode(zomato_events) u as z").drop("offers", "zomato_events")
    query1.createOrReplaceTempView("tab2")
    val query3 = spark.sql("select *, `y`.`photo`.* from tab2 lateral view explode(photos) D as y").drop("o", "z")
    query3.createOrReplaceTempView("tab3")
    val query4 = spark.sql("select * from tab3").drop("photos", "y")
    query4.createOrReplaceTempView("tab4")
    val query5 = spark.sql("select *, `R`.`res_id`, apikey, `average_cost_for_two`, `book_url`, cuisines, currency, deeplink, " +
      "`events_url`, `featured_image`, `has_online_delivery`, `has_table_booking`, id, `is_delivering_now` from tab4")
    query5.show(5, false)
    query5.printSchema()
    val host = "jdbc:oracle:thin:@//oracledb.conbyj3qndaj.ap-south-1.rds.amazonaws.com:1521/ORACLED"
    val prop = new java.util.Properties()
    prop.setProperty("User", "ousername")
    prop.setProperty("password", "opassword")
    prop.setProperty("driver", "oracle.jdbc.driver.OracleDriver")
    //Above code illustrates, oracledb connection properties
    query5.write.mode(SaveMode.Overwrite).jdbc(host, output, prop)
    //Above code illustrates, writing data to Oracledb
    spark.stop()
  }
}

  /*
  Summary :
  --------
  I have complex JSON 'zomato' data. I am trying to process(clean) the data and want to store this to ORACLEDB.

  dependency :
-----------
Add oracle jdbc jar file.

Spark Submit command :
--------------------
spark-submit
  --class com.bigdata.spark.sparksql.ComplexJSONDataProcessing
  --master local
  --deploy-mode client
  --jars s3://athoutam/Driver/ojdbc7.jar
  s3://athoutam/Jar/spark-project_2.11-0.1.jar
  s3://athoutam/Input/zomato/*.json zomatodata
  
   */
    


