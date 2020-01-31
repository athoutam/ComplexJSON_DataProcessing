package com.bigdata.spark.sparksql

import org.apache.spark.sql.SparkSession

import org.apache.spark.sql._

object posexplode_JSON {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("posexplode_JSON").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("posexplode_JSON").getOrCreate()
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
    val input = "C:\\work\\datasets\\JSON\\posexplode.json"
    val df = spark.read.format("json").option("inferSchema","true").load(input)
    df.createOrReplaceTempView("tab")
    val res = spark.sql("select * from tab lateral view explode(city) A as c lateral view explode(email) B as e lateral view explode(phone) C as p").drop("city","email","phone")
    res.show()
    res.printSchema()
//    val res1 = spark.sql("select name, NewCity, NewPhone, email from tab lateral view posexplode(city) D as d, NewCity lateral view posexplode(phone) E as e, NewPhone where d == e")
//    res1.show(false)
//    res1.printSchema()
    spark.stop()
  }
}


/*
Summary :
--------
Lateral View POSExplode :
=========================
I want to map first phone number to first city and second with second one, and that for all the records. Then we can use posexplode (positional explode)
posexplode gives you an index along with value when you expand any error, and then you can use this indexes to map values with each other as mentioned above.

Lateral view Explode :
=====================
Lateral view explode, explodes the array data into multiple rows. for example, let’s say our table look like this, where phone is an array of Int.

+----------+--------------------+-------+---------------+
|      city|               email|   name|          phone|
+----------+--------------------+-------+---------------+
|[del, mas]|[venu@gmail.com, ...|   venu|[999999, 22222]|
|[del, mas]|[venu@gmail.com, ...|    anu|[999999, 22222]|
|[del, mas]|[venu@gmail.com, ...|  satya|[999999, 22222]|
|[del, mas]|[venu@gmail.com, ...| nirmal|[999999, 22222]|
|[del, mas]|[venu@gmail.com, ...|nischal|[999999, 22222]|
+----------+--------------------+-------+---------------+

Applying a lateral view explode on the above table will expand the both phone and city and do a cross join, your final table will look like this.

+-----+---+---------------+------+
| name|  c|              e|     p|
+-----+---+---------------+------+
| venu|del| venu@gmail.com|999999|
| venu|del| venu@gmail.com| 22222|
| venu|del|venuk@test.com |999999|
| venu|del|venuk@test.com | 22222|
| venu|mas| venu@gmail.com|999999|
| venu|mas| venu@gmail.com| 22222|
| venu|mas|venuk@test.com |999999|
| venu|mas|venuk@test.com | 22222|
|  anu|del| venu@gmail.com|999999|
|  anu|del| venu@gmail.com| 22222|
|  anu|del|venuk@test.com |999999|
|  anu|del|venuk@test.com | 22222|
|  anu|mas| venu@gmail.com|999999|
|  anu|mas| venu@gmail.com| 22222|
|  anu|mas|venuk@test.com |999999|
|  anu|mas|venuk@test.com | 22222|
|satya|del| venu@gmail.com|999999|
|satya|del| venu@gmail.com| 22222|
|satya|del|venuk@test.com |999999|
|satya|del|venuk@test.com | 22222|
+-----+---+---------------+------+
only showing top 20 rows

root
 |-- name: string (nullable = true)
 |-- c: string (nullable = true)
 |-- e: string (nullable = true)
 |-- p: long (nullable = true)
 */