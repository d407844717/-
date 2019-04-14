package com.bigdata.ml.streaming

import org.slf4j.LoggerFactory
import kafka.serializer.{Decoder, StringDecoder}
import kafka.utils.VerifiableProperties
import org.apache.avro.io.{DatumReader, DecoderFactory}
import org.apache.avro.specific.SpecificDatumReader
import org.apache.commons.cli._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkException, TaskContext}
import org.apache.spark.sql.{Row, SQLContext};
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer,IDFModel}
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.sql.{Row, SQLContext};
import org.apache.spark.ml.clustering.KMeansModel
import com.smart.ml.entity.{SalesPurchasing,SupplyProduct}
import com.bigdata.ml.record.{SalesPurchasingToRelation,SupplyProductKafka,ProductType,SalesPurchasingKafka,SalesPurchasingRes,SalesPurchasingPreRes}
import com.bigdata.ml.serialize.{SupplyProductDecoder,IteblogEncoder,SalesPurchasingDecoder} 
import kafka.producer.KeyedMessage;  
import kafka.producer.ProducerConfig;  
import java.util.Properties; 
import kafka.serializer.{StringEncoder}
import kafka.producer.Producer
import com.mongodb.spark._;


object SalesPurchasingPreStreaming {
    val log = LoggerFactory.getLogger(SalesPurchasingPreStreaming.getClass)
    
    val APP_NAME="daas_ml_streaming_salespurchasing_pre_test";
        
    var kafkaParams:Map[String,String]=_;
    var kafkaTopics:Map[String,Int]=_;
    var duration:Long=1;
    
   
    def streamingAnalyse():Unit={
      val conf: SparkConf = new SparkConf().setAppName(APP_NAME);
      conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
       conf.set("spark.mongodb.output.uri", "mongodb://10.26.12.227:9927/icms.sales_purchasing")
      //docker mongodb
     //  conf.set("spark.mongodb.output.uri", "mongodb://10.251.131.33:9927/icms.sales_purchasing")
   //   log.info("Setup StreamingContext with duration {} seconds.", duration);
      
      val ssc: StreamingContext = new StreamingContext(conf, Seconds.apply(duration))
      analyseProcess(ssc);     
      ssc.start();
      ssc.awaitTermination();
      ssc.stop()
    }
    
    def analyseProcess(ssc: StreamingContext): Unit = {
      log.info("KafkaParams: [{}]", kafkaParams)
      log.info("KafkaTopics: [{}]", kafkaTopics)
      val stream: DStream[SalesPurchasing] = KafkaUtils.createStream[String, SalesPurchasing, StringDecoder,
            SalesPurchasingDecoder](ssc, kafkaParams, kafkaTopics, StorageLevel.MEMORY_ONLY).map(record => record._2).persist();
      modelAnalyse(stream);
    }
    //转换stream为rdd,加载idf以及kmeans的model
    def modelAnalyse(dStream:DStream[SalesPurchasing]):Unit={
     log.info("start......");
      
      val datas=dStream.foreachRDD { rdd=>
         val sqlContext = SQLContextSingletonPre.getInstance(rdd.sparkContext);
         import sqlContext.implicits._
        var wordsDataFrame=rdd.map { x =>
             SalesPurchasingKafka(
                  x.getProductId.toString().toLong,
                  x.getProductName.toString(),
                  x.getPrice.toString(),
                  x.getCategory.toString(),
                  x.getProductInfo.toString(),
                  x.getProductContacts.toString(),
                  x.getContactPhone.toString(),
                  x.getCompanyName.toString(),
                  x.getAddress.toString(),
                  x.getSpecification.toString(),
                  x.getQuantity.toString(),
                  x.getPacking.toString(),
                  x.getCreatorUser.toString(),
                  x.getDirection.toString().toLong,
                  x.getProductInfoik.toString()
                 )
            }.toDF()
            log.info("...............................................")
               log.info("...............................................")
                  log.info("...............................................")
           wordsDataFrame.show()
              log.info("...............................................")
                 log.info("...............................................")
                    log.info("...............................................")
                       log.info("...............................................")
       //转换为kmeans计算格式
      // val trainDataRdd=wordsDataFrame.select($"product_id",$"product_name",$"price",$"category",$"product_info",$"product_contacts",$"contact_phone",$"company_name",$"address",$"specification",$"quantity",$"packing",$"creator_user",$"direction");
      
   if(wordsDataFrame!=null)
   {
      log.info("...............................................data num is："+wordsDataFrame.count())
     //  加载kmeansmodel
    if(wordsDataFrame.count()>0){
        log.info("...............................................result..................")
       val salesPurchasingPreData=wordsDataFrame.map{x=>
         SalesPurchasingPreRes(
               x.get(0).toString().toLong,
               x.get(1).toString(),
               x.get(2).toString(),
               x.get(3).toString(),
               System.currentTimeMillis().toString(),
               x.get(4).toString(),
               x.get(5).toString(),
               x.get(6).toString(),
               x.get(7).toString(),
               x.get(8).toString(),
               x.get(9).toString(),
               x.get(10).toString(),
               x.get(11).toString(),
               System.currentTimeMillis(),
               x.get(12).toString(),
               System.currentTimeMillis(),
               1,
               System.currentTimeMillis(),
               x.get(13).toString().toLong
             )
         }.toDF();
      if(salesPurchasingPreData!=null){
         log.info("...............................................write mongodb..................")
          MongoSpark.save(salesPurchasingPreData);
          wordsDataFrame.foreach { x => 
              if(x!=null){
                log.info("...............................................write kafka..................")
                sendMessage(SalesPurchasingToRelation(
                      x.get(0).toString().toLong,
                      x.get(1).toString(),
                      x.get(4).toString(),
                      x.get(7).toString(),
                      x.get(13).toString().toLong,
                      x.get(14).toString()
                    ));
              //  sendMessage(SupplyProductRes(x.get(0).toString().toInt,x.get(8).toString(),x.get(18).toString().toInt,x.get(19).toString().toInt))
              }
           }
        }
       }
      }
     }
   }
    
    def toInt(s: String): Int = {
      scala.util.Try(s.toInt).getOrElse(0);
    }
    
    def getProducerConfig():Properties={
      val props=new Properties()
     // props.put("metadata.broker.list","10.251.50.91:6667,10.163.123.205:6667")
      props.put("metadata.broker.list","broker-0.kafka.mesos:9945,broker-1.kafka.mesos:9990,broker-2.kafka.mesos:9331")
      props.put("serializer.class", classOf[IteblogEncoder[SalesPurchasingToRelation]].getName)
      props.put("key.serializer.class", classOf[StringEncoder].getName)
      props
    }

    
     def sendMessage(message:SalesPurchasingToRelation){
      val producer=new Producer[String,SalesPurchasingToRelation](
          new ProducerConfig(getProducerConfig()))
          if(message!=null){
       producer.send(
           new KeyedMessage[String,SalesPurchasingToRelation]("smart_salespurchasing_relation_test",message))
          }
       producer.close
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
}

object SQLContextSingletonPre{
 
  @transient private var instance:SQLContext=_;
  
  def getInstance(sparkContext:SparkContext):SQLContext={
    if(instance==null){
      instance=new SQLContext(sparkContext);
    }
    instance
  }
}