package com.bigdata.spark.sparksql

import org.apache.spark.sql.SparkSession

import org.apache.spark.sql._

object WorldBank_JSON {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("WorldBank_JSON").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("WorldBank_JSON").getOrCreate()
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
    val data = args(0)
    val output = args(1)
    val df = spark.read.format("json").option("inferSchema","true").load(data)
    df.createOrReplaceTempView("tab")
        val query = "select `_id`.`$oid` as oid, approvalfy," +
          "`board_approval_month`," +
          "boardapprovaldate," +
          "borrower," +
          "closingdate," +
          "`country_namecode`," +
          "countrycode," +
          "countryname," +
          "countryshortname," +
          "docty,envassesmentcategorycode,grantamt," +
          "ibrdcommamt,id,idacommamt,impagency,lendinginstr,lendinginstrtype," +
          "lendprojectcost," +
          "mp.Name as mpname, mp.Percent as mppercent," +
          "mj.code as mjcode, mj.name as mjname," +
          "mjt.code as mjtcode, mjt.name as mjtname," +
          "mjthemecode,prodline,prodlinetext,productlinetype," +
          "`project_name`," +
          "pd.DocDate,pd.DocType,pd.DocTypeDesc,pd.DocURL,pd.EntityID," +
          "projectfinancialtype,projectstatusdisplay,regionname," +
          "s.Name as sectorname," +
          "sector1.Name as s1name,sector1.Percent as s1percent," +
          "sector2.Name as s2name,sector2.Percent as s2percent," +
          "sector3.Name as s3name,sector3.Percent as s3percent," +
          "sector4.Name as s4name,sector4.Percent as s4percent," +
          "snc.code as snccode,snc.name as sncname," +
          "sectorcode,source,status,supplementprojectflg," +
          "theme1.name as theme1name,theme1.Percent as theme1Per," +
          "thn.code as thenamecode,thn.name as themename," +
          "themecode,totalamt,totalcommamt,url " +
          "from tab  " +
          "lateral view explode(majorsector_percent) tmp as mp " +
          "lateral view explode(mjsector_namecode) tmp1 as mj " +
          "lateral view explode(mjtheme_namecode) tmp2 as mjt " +
          "lateral view explode(projectdocs) tmp3 as pd " +
          "lateral view explode(sector) tmp4 as s " +
          "lateral view explode(sector_namecode) tmp5 as snc " +
          "lateral view explode(theme_namecode) tmp6 as thn"

        val res = spark.sql(query)

    res.show(10, false)
    res.printSchema()
    res.write.format("csv").option("header","true").option("inferSchema","true").save(output)
    spark.stop()
  }
}

/*
Summary :
---------
I have JSON data in AWS S3. Bringing data from there and processing (cleaning) it and then storing this back to S3.

Spark Submit command :
--------------------
spark-submit
  --class com.bigdata.spark.sparksql.WorldBank_JSON
  --master local
  --deploy-mode client
  file://home/hadoop/sparkpocs_2.11-0.1.jar
  s3://athoutam/Input/WorldBank_JSONData/*.json
  s3://athoutam/Output/WorldBank_JSONData
  
 */
 