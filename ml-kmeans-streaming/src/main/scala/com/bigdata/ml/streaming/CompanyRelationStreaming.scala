package com.bigdata.ml.streaming
import org.apache.commons.cli._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkException, TaskContext}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import com.bigdata.ml.record.{SupplyProductRes,CompanyRelation}
import kafka.serializer.{StringDecoder}
import com.bigdata.ml.serialize.{IteblogDecoder} 
import org.apache.spark.storage.StorageLevel
import org.slf4j.LoggerFactory
import org.apache.spark.sql.{Row, SQLContext};
import org.apache.spark.{SparkConf, SparkContext}
import com.bigdata.ml.mongodb.MongodbSingleton;
import com.mongodb.casbah.{WriteConcern => MongodbWriteConcern}
import com.stratio.datasource._
import com.stratio.datasource.mongodb._
import com.stratio.datasource.mongodb.schema._
import com.stratio.datasource.mongodb.writer._
import com.stratio.datasource.mongodb.config._
import com.stratio.datasource.mongodb.config.MongodbConfig._
import com.stratio.datasource.util.Config._

object CompanyRelationStreaming {
  val log = LoggerFactory.getLogger(CompanyRelationStreaming.getClass)
  
   val APP_NAME="daas_ml_streaming_companyrelation_pro";
   
   var kafkaParams:Map[String,String]=_;
   var kafkaTopics:Map[String,Int]=_;
   var duration:Long=1;
   
     def streamingAnalyse():Unit={
      val conf: SparkConf = new SparkConf().setAppName(APP_NAME);
      conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
      log.info("Setup StreamingContext with duration {} seconds.", duration);
      
      val ssc: StreamingContext = new StreamingContext(conf, Seconds.apply(duration))
      analyseProcess(ssc);     
      ssc.start();
      ssc.awaitTermination();
      ssc.stop()
    }
   implicit def doubleToInt(x:Double) = x toInt
   def analyseProcess(ssc: StreamingContext): Unit = {
      log.info("KafkaParams: [{}]", kafkaParams)
      log.info("KafkaTopics: [{}]", kafkaTopics)
      val stream= KafkaUtils.createStream[String, SupplyProductRes, StringDecoder,
            IteblogDecoder[SupplyProductRes]](ssc, kafkaParams, kafkaTopics, StorageLevel.MEMORY_ONLY).map(record => record._2);
      
     stream.foreachRDD{rdd=>      
        val sqlContext = SQLContextCRSingleton.getCRInstance(rdd.sparkContext);            
           val num=rdd.count()
           if(num>0){
             for(line<-rdd.collect()){
                  val builder = MongodbSingleton.getInstanceS()    
                  var mongoRDD = sqlContext.fromMongoDB(builder.build())   
                  mongoRDD.registerTempTable("salespurchasing") 
                 var spResult=sqlContext.sql("select product_id from salespurchasing where company_name='"+line.company_name+"' and direction="+line.direction+" and category_cal="+line.category+"");
                 if(spResult.count()<=1){
                      var direction= -line.direction
                      var crResult=sqlContext.sql("select company_name,category_cal from salespurchasing where category_cal="+line.category+" and direction="+direction+" and company_name!='"+line.company_name+"'")
                                   .distinct();
                      import sqlContext.implicits._
                     // crResult.show(100)
                      var crDataFrame=crResult.map { crline =>
                           direction = if( direction == 1 ) 0 else 1; // -1->1 1->0
                           CompanyRelation(line.company_name,crline.get(0).toString(),0,direction,0,1,System.currentTimeMillis(),System.currentTimeMillis())
                           direction = if( direction == 1 ) 0 else 1; // 1->0 0->1
                           CompanyRelation(crline.get(0).toString(),line.company_name,0,direction,0,1,System.currentTimeMillis(),System.currentTimeMillis())
                     }.toDF()
                    val builderR=MongodbSingleton.getInstanceR();
                    crDataFrame.saveToMongodb(builderR.build())                 
                 }
             }
           }
       }
    }
     
   def main(args:Array[String]):Unit={
       val zookeepers=args(0).toString();
     
      val groupId=args(1).toString();
     
      val topics=args(2).toString().split(",");
    
      duration=toInt(args(3).toString());
      
      kafkaParams=Map[String,String]("zookeeper.connect" -> zookeepers, "group.id" -> groupId);
      topics.foreach { i =>
         kafkaTopics = Map[String, Int](i -> 1)
       }
      
      streamingAnalyse()

    }
       def toInt(s: String): Int = {
      scala.util.Try(s.toInt).getOrElse(0);
    }
}
object SQLContextCRSingleton{
 
  @transient private var crinstance:SQLContext=_;
  
  def getCRInstance(sparkContext:SparkContext):SQLContext={
    if(crinstance==null){
      crinstance=new SQLContext(sparkContext);
    }
    crinstance
  }
}