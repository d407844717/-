package com.bigdata.ml.streaming

import org.slf4j.LoggerFactory
import kafka.serializer.{Decoder, StringDecoder}
import kafka.utils.VerifiableProperties
import com.bigdata.ml.serialize.{IteblogDecoder} 
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
import com.bigdata.ml.record.{SalesPurchasingToRelation,SupplyProductRes,SalesPurchasingKafka,SalesPurchasingRes,SalesPurchasingRelation}
//import com.smart.ml.mongodb.MongodbSingleton;
//import com.mongodb.casbah.{WriteConcern => MongodbWriteConcern}
//import com.stratio.datasource._
//import com.stratio.datasource.mongodb._
//import com.stratio.datasource.mongodb.schema._
//import com.stratio.datasource.mongodb.writer._
//import com.stratio.datasource.mongodb.config._
//import com.stratio.datasource.mongodb.config.MongodbConfig._
//import com.stratio.datasource.util.Config._
import com.bigdata.ml.serialize.{SupplyProductDecoder,IteblogEncoder,SalesPurchasingDecoder} 
import kafka.producer.KeyedMessage;  
import kafka.producer.ProducerConfig;  
import java.util.Properties; 
import kafka.serializer.{StringEncoder}
import kafka.producer.Producer
import com.mongodb.spark._;


object SalesPurchasingBehStreaming {
    val log = LoggerFactory.getLogger(SalesPurchasingBehStreaming.getClass)
    
    val APP_NAME="daas_ml_streaming_salespurchasing_beh_test";
    var IDF_MODEL_PATH="hdfs://10.174.229.47:8020/ml/test/model/idfmodel/idfmodel14000_16000";
    var KMEANS_MODEL_PATH="hdfs://10.174.229.47:8020/ml/test/model/kmeansmodel/kmeansmodel14000_16000";
    
    var tfNumFeatures = 16000;
    
    var kafkaParams:Map[String,String]=_;
    var kafkaTopics:Map[String,Int]=_;
    var duration:Long=1;
    
   
    def streamingAnalyse():Unit={
      val conf: SparkConf = new SparkConf().setAppName(APP_NAME);
      conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
//       conf.set("spark.mongodb.input.uri", "mongodb://10.26.12.227:9927/icms.sales_purchasing_relation?readPreference=primaryPreferred");
//       conf.set("spark.mongodb.output.uri", "mongodb://10.26.12.227:9927/icms.test")
      //docker mongdob
        conf.set("spark.mongodb.input.uri", "mongodb://10.251.131.33:9927/icms.sales_purchasing_relation?readPreference=primaryPreferred");
       conf.set("spark.mongodb.output.uri", "mongodb://10.251.131.33:9927/icms.sales_purchasing_relation")
   //   log.info("Setup StreamingContext with duration {} seconds.", duration);
      
      val ssc: StreamingContext = new StreamingContext(conf, Seconds.apply(duration))
    //  @transient val sqlContext: SQLContext = SQLContextSingleton.getInstance(ssc.sparkContext);
      analyseProcess(ssc);     
      ssc.start();
      ssc.awaitTermination();
      ssc.stop()
    }
    
    def analyseProcess(ssc: StreamingContext): Unit = {
      log.info("KafkaParams: [{}]", kafkaParams)
      log.info("KafkaTopics: [{}]", kafkaTopics)
      val stream: DStream[SalesPurchasingToRelation] = KafkaUtils.createStream[String, SalesPurchasingToRelation, StringDecoder,
             IteblogDecoder[SalesPurchasingToRelation]](ssc, kafkaParams, kafkaTopics, StorageLevel.MEMORY_ONLY).map(record => record._2).persist();
      
      modelAnalyse(stream);
    }
    //转换stream为rdd,加载idf以及kmeans的model
    def modelAnalyse(dStream:DStream[SalesPurchasingToRelation]):Unit={
     log.info("idfModel start......");
   
    val datas=dStream.foreachRDD { rdd=>
        val sqlContext = SQLContextSingleton.getInstance(rdd.sparkContext);
         import sqlContext.implicits._
        var wordsDataFrame=rdd.toDF()
       // MongoSpark.save(wordsDataFrame)
        //计算idf
   if(wordsDataFrame.count()>0){
       val wordsData = new Tokenizer()
           .setInputCol("product_infoik")
           .setOutputCol("product_words")
           .transform(wordsDataFrame)
       val tfData = new HashingTF()
           .setNumFeatures(tfNumFeatures)
           .setInputCol("product_words")
           .setOutputCol("productFeatures")
           .transform(wordsData)
       val idfModel= IDFModelSingleton.getInstance(IDF_MODEL_PATH);
       val idfData=idfModel.transform(tfData)
       //转换为kmeans计算格式
       val trainDataRdd=idfData.select($"product_id",$"product_name",$"product_info",$"company_name",$"direction",$"features");
      
   if(trainDataRdd!=null)
   {
      log.info("...............................................data num is："+trainDataRdd.count())
     //  加载kmeansmodel
    if(trainDataRdd.count()>0){
        log.info("...............................................result..................")
       val kmeansModel=KMEANSModelSingleton.getInstance(KMEANS_MODEL_PATH);
       var kmeansData=kmeansModel.transform(trainDataRdd);
//       val kmeansRes=kmeansData.map{x=>
//         
//         }.toDF();
       for(x<-kmeansData.collect()){
//         MongoSpark.save(kmeansData)
//         line.ge 
//       }
//        kmeansData.foreach { x =>  
            var directionInt=x.get(4).toString().toLong
                if(directionInt== -1){
                  directionInt=1
                }else if(directionInt== 1){
                  directionInt=0
                }else{
                  directionInt=2
                }
            if(x!=null){
               val salespurchasings_relations=MongoSpark.load(sqlContext).toDF()
              salespurchasings_relations.registerTempTable("salespurchasings_relation") 
              val dataFrame=sqlContext.sql("select rel_product_id,rel_product_name,rel_company_name,rel_product_info,direction,category_cal from salespurchasings_relation where "+
                            "category_cal="+x.get(6).toString().toLong+" and direction ="+directionInt+" and "+
                             "rel_company_name!='"+x.get(3).toString()+"'").dropDuplicates("rel_product_id");
              val dataF1=dataFrame.map { y =>  
                          SalesPurchasingRelation(
                              x.get(0).toString().toLong,
                              y.get(0).toString().toLong,
                              x.get(1).toString(),
                              y.get(1).toString(),
                              x.get(3).toString(),
                              y.get(2).toString(),
                              y.get(3).toString(),
                              directionInt,
                              y.get(5).toString().toLong,
                              System.currentTimeMillis(),
                              System.currentTimeMillis()
                           )
                       }.toDF()
               
                val dataF2=dataFrame.map { z =>  
                          SalesPurchasingRelation(
                              z.get(0).toString().toLong,
                              x.get(0).toString().toLong,
                              z.get(1).toString(),
                              x.get(1).toString(),
                              z.get(2).toString(),
                              x.get(3).toString(),
                              x.get(2).toString(),
                              direction = if( directionInt == 1 ) 0 else 1,
                              z.get(5).toString().toLong,
                              System.currentTimeMillis(),
                              System.currentTimeMillis()
                           )
                       }.toDF()               
                MongoSpark.save(dataF2.unionAll(dataF1));
            }
//          }
//      if(kmeansData!=null){
//         log.info("...............................................write mongodb..................")
////        val builder = MongodbSingleton.getInstanceS()
////        kmeansRes.saveToMongodb(builder.build());
//          //MongoSpark.save(kmeansRes);
//         kmeansData.foreach { x => 
//              if(x!=null){
//                log.info("...............................................write kafka..................")
//                sendMessage(SupplyProductRes(x.get(0).toString().toLong,x.get(3).toString(),x.get(4).toString().toLong,x.get(6).toString().toLong))
//              }
//           }
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
      props.put("metadata.broker.list","10.251.50.91:6667,10.163.123.205:6667")
      props.put("serializer.class", classOf[IteblogEncoder[SupplyProductRes]].getName)
      props.put("key.serializer.class", classOf[StringEncoder].getName)
      props
    }
    
//    def sendMessages(messages:List[SupplyProductRes]){
//      val producer=new Producer[String,SupplyProductRes](
//          new ProducerConfig(getProducerConfig()))
//          if(messages.length>0){
//       producer.send(messages.map {
//           new KeyedMessage[String,SupplyProductRes](cRTopics,"cr",_)}:_*)
//          }
//       producer.close
//    }
    
     def sendMessage(message:SupplyProductRes){
      val producer=new Producer[String,SupplyProductRes](
          new ProducerConfig(getProducerConfig()))
          if(message!=null){
       producer.send(
           new KeyedMessage[String,SupplyProductRes]("smart_sy_cr",message))
          }
       producer.close
    }
    
    def main(args:Array[String]):Unit={
     
      val zookeepers=args(0).toString();
     
      val groupId=args(1).toString();
     
      val topics=args(2).toString().split(",");
     
      duration=toInt(args(3).toString());
      
      IDF_MODEL_PATH=args(4).toString();
      
      KMEANS_MODEL_PATH=args(5).toString();
      
      tfNumFeatures=toInt(args(6).toString())
      
      kafkaParams=Map[String,String]("zookeeper.connect" -> zookeepers, "group.id" -> groupId);
      topics.foreach { i =>
         kafkaTopics = Map[String, Int](i -> 1)
       }
      
      streamingAnalyse()

    }
}

object SQLContextSingleton{
 
  @transient private var instance:SQLContext=_;
  
  def getInstance(sparkContext:SparkContext):SQLContext={
    if(instance==null){
      instance=new SQLContext(sparkContext);
    }
    instance
  }
}
object IDFModelSingleton{
  
  @transient private var instance:IDFModel=_;
  
  def getInstance(idfPath:String):IDFModel={
    if(instance==null){
      instance= IDFModel.load(idfPath)
    }
    instance
  }
}
object KMEANSModelSingleton{
  
  @transient private var instance:KMeansModel=_;
  
  def getInstance(kmeansPath:String):KMeansModel={
    if(instance==null){
      instance=KMeansModel.load(kmeansPath)
    }
    instance
  }
}
