package com.bigdata.ml.mongodb
import com.mongodb.spark._;
import org.apache.spark.sql.SQLContext
import com.stratio.datasource.util.Config._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import  com.bigdata.ml.record.SalesPurchasingRelation
import org.slf4j.LoggerFactory

object ProductTypeToSalesPurchasingRelation extends Serializable{
    val log = LoggerFactory.getLogger(ProductTypeToSalesPurchasingRelation.getClass)
  val APP_NAME="daas_ml_producttype_salespurchasingrelation_test";
   def main(args:Array[String]){
      analyse();
  }
   def analyse():Unit={
     @transient
     val conf=new SparkConf()
            .setAppName(APP_NAME)
           .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
     conf.set("spark.mongodb.input.uri", "mongodb://10.26.12.227:9927/icms.sales_purchasing?readPreference=primaryPreferred");
      conf.set("spark.mongodb.output.uri", "mongodb://10.26.12.227:9927/icms.sales_purchasing_relation")
       conf.set("spark.mongodb.keep_alive_ms", "10000000")
     @transient
      val sc=new SparkContext(conf);
      @transient
      val sqlContext: SQLContext = new SQLContext(sc)
      readMongodb(sqlContext,sc);
      sc.stop();
   }
   def readMongodb(sqlContext:SQLContext,sc:SparkContext):Unit={
 
     
      val salespurchasings=MongoSpark.load(sqlContext).toDF()
      salespurchasings.registerTempTable("salespurchasing") 
     val dataFrame=sqlContext.sql("select b.product_id,c.product_id,b.product_name,c.product_name,b.company_name,c.company_name,c.product_info,b.direction,b.category_cal"+
              " from (SELECT product_id,product_name,company_name,product_info,category_cal,direction  FROM salespurchasing) b"+
              ",(SELECT product_id,product_name,company_name,product_info,category_cal,direction  FROM salespurchasing) c "+
              "where b.product_id!=c.product_id and b.direction!=c.direction and b.category_cal=c.category_cal").persist();

      import sqlContext.implicits._; 
      log.info("........................................................")
       log.info("........................................................")
        log.info("........................................................")
         log.info("........................................................")
          log.info("........................................................")
           log.info("........................................................PRE NUM IS"+dataFrame.count())
                log.info("........................................................")
       log.info("........................................................")
        log.info("........................................................")
         log.info("........................................................")
          log.info("........................................................")
    
    val dataframe=dataFrame.map { x =>
      var directionInt=x.get(7).toString().toLong
                if(directionInt== -1){
                  directionInt=1
                }else if(directionInt== 1){
                  directionInt=0
                }else{
                  directionInt=2
                }
        SalesPurchasingRelation(
                  x.get(0).toString().toLong,
                  x.get(1).toString().toLong,
                  x.get(2).toString(),
                  x.get(3).toString(),
                  x.get(4).toString(),
                  x.get(5).toString(),
                  x.get(6).toString(),
                  directionInt,
                  x.get(8).toString().toLong,
                  System.currentTimeMillis(),
                  System.currentTimeMillis()
                 )
      }.toDF()
      log.info("........................................................")
       log.info("........................................................")
        log.info("........................................................")
         log.info("........................................................")
          log.info("........................................................")
           log.info("........................................................NUM IS"+dataframe.count())
                log.info("........................................................")
       log.info("........................................................")
        log.info("........................................................")
         log.info("........................................................")
          log.info("........................................................")
     MongoSpark.save(dataframe)
   }
}