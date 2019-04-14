//package com.bigdata.ml.streaming
//
//import org.slf4j.LoggerFactory
//import kafka.serializer.{Decoder, StringDecoder}
//import kafka.utils.VerifiableProperties
//import org.apache.avro.io.{DatumReader, DecoderFactory}
//import org.apache.avro.specific.SpecificDatumReader
//import org.apache.commons.cli._
//import org.apache.spark.storage.StorageLevel
//import org.apache.spark.streaming.dstream.DStream
//import org.apache.spark.streaming.kafka.KafkaUtils
//import org.apache.spark.streaming.{Seconds, StreamingContext}
//import org.apache.spark.{SparkConf, SparkException, TaskContext}
//import org.apache.spark.sql.{Row, SQLContext};
//import org.apache.spark.{SparkConf, SparkContext}
//import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer,IDFModel}
//import org.apache.spark.ml.linalg.{Vector, Vectors}
//import org.apache.spark.sql.{Row, SQLContext};
//import org.apache.spark.ml.clustering.KMeansModel
//import com.smart.ml.entity.{SalesPurchasing,SupplyProduct}
//import com.yonyou.ml.record.{SupplyProductRes,SupplyProductKafka,ProductType,SalesPurchasingKafka,SalesPurchasingRes}
////import com.smart.ml.mongodb.MongodbSingleton;
////import com.mongodb.casbah.{WriteConcern => MongodbWriteConcern}
////import com.stratio.datasource._
////import com.stratio.datasource.mongodb._
////import com.stratio.datasource.mongodb.schema._
////import com.stratio.datasource.mongodb.writer._
////import com.stratio.datasource.mongodb.config._
////import com.stratio.datasource.mongodb.config.MongodbConfig._
////import com.stratio.datasource.util.Config._
//import com.smart.ml.serialize.{SupplyProductDecoder,IteblogEncoder,SalesPurchasingDecoder} 
//import kafka.producer.KeyedMessage;  
//import kafka.producer.ProducerConfig;  
//import java.util.Properties; 
//import kafka.serializer.{StringEncoder}
//import kafka.producer.Producer
//import com.mongodb.spark._;
//
//
//object SalesPurchasingStreaming {
//    val log = LoggerFactory.getLogger(SalesPurchasingStreaming.getClass)
//    
//    val APP_NAME="daas_ml_streaming_salespurchasing_pro";
//    var IDF_MODEL_PATH="hdfs://10.174.229.47:8020/ml/test/model/idfmodel/idfmodel10000_12000";
//    var KMEANS_MODEL_PATH="hdfs://10.174.229.47:8020/ml/test/model/kmeansmodel/kmeansmodel10000_12000";
//    
//    var tfNumFeatures = 12000;
//    
//    var kafkaParams:Map[String,String]=_;
//    var kafkaTopics:Map[String,Int]=_;
//    var duration:Long=1;
//    
//   
//    def streamingAnalyse():Unit={
//      val conf: SparkConf = new SparkConf().setAppName(APP_NAME);
//      conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
//       conf.set("spark.mongodb.output.uri", "mongodb://10.26.13.4:9927/icms.sales_purchasing")
//   //   log.info("Setup StreamingContext with duration {} seconds.", duration);
//      
//      val ssc: StreamingContext = new StreamingContext(conf, Seconds.apply(duration))
//      analyseProcess(ssc);     
//      ssc.start();
//      ssc.awaitTermination();
//      ssc.stop()
//    }
//    
//    def analyseProcess(ssc: StreamingContext): Unit = {
//      log.info("KafkaParams: [{}]", kafkaParams)
//      log.info("KafkaTopics: [{}]", kafkaTopics)
//      val stream: DStream[SalesPurchasing] = KafkaUtils.createStream[String, SalesPurchasing, StringDecoder,
//            SalesPurchasingDecoder](ssc, kafkaParams, kafkaTopics, StorageLevel.MEMORY_ONLY).map(record => record._2).persist();
//      modelAnalyse(stream);
//    }
//    //转换stream为rdd,加载idf以及kmeans的model
//    def modelAnalyse(dStream:DStream[SalesPurchasing]):Unit={
//     log.info("idfModel start......");
//      
//      val datas=dStream.foreachRDD { rdd=>
//         val sqlContext = SQLContextSingleton.getInstance(rdd.sparkContext);
//         import sqlContext.implicits._
//        var wordsDataFrame=rdd.map { x =>
//             SalesPurchasingKafka(
//                  x.getProductId.toString().toLong,
//                  x.getProductName.toString(),
//                  x.getPrice.toString(),
//                  x.getCategory.toString(),
//                  x.getProductInfo.toString(),
//                  x.getProductContacts.toString(),
//                  x.getContactPhone.toString(),
//                  x.getCompanyName.toString(),
//                  x.getAddress.toString(),
//                  x.getSpecification.toString(),
//                  x.getQuantity.toString(),
//                  x.getPacking.toString(),
//                  x.getCreatorUser.toString(),
//                  x.getDirection.toString().toLong,
//                  x.getProductInfoik.toString()
//                 )
//            }.toDF()
//        //计算idf
//       val wordsData = new Tokenizer()
//           .setInputCol("product_infoik")
//           .setOutputCol("product_words")
//           .transform(wordsDataFrame)
//       val tfData = new HashingTF()
//           .setNumFeatures(tfNumFeatures)
//           .setInputCol("product_words")
//           .setOutputCol("productFeatures")
//           .transform(wordsData)
//       val idfModel= IDFModelSingleton.getInstance(IDF_MODEL_PATH);
//       val idfData=idfModel.transform(tfData)
//       //转换为kmeans计算格式
//       val trainDataRdd=idfData.select($"product_id",$"product_name",$"price",$"category",$"product_info",$"product_contacts",$"contact_phone",$"company_name",$"address",$"specification",$"quantity",$"packing",$"creator_user",$"direction",$"features");
//      
//   if(trainDataRdd!=null)
//   {
//      log.info("...............................................data num is："+trainDataRdd.count())
//     //  加载kmeansmodel
//    if(trainDataRdd.count()>0){
//        log.info("...............................................result..................")
//       val kmeansModel=KMEANSModelSingleton.getInstance(KMEANS_MODEL_PATH);
//       var kmeansData=kmeansModel.transform(trainDataRdd);
//       val kmeansRes=kmeansData.map{x=>
//         SalesPurchasingRes(
//               x.get(0).toString().toLong,
//               x.get(1).toString(),
//               x.get(2).toString(),
//               x.get(3).toString(),
//               System.currentTimeMillis().toString(),
//               x.get(4).toString(),
//               x.get(5).toString(),
//               x.get(6).toString(),
//               x.get(7).toString(),
//               x.get(8).toString(),
//               x.get(9).toString(),
//               x.get(10).toString(),
//               x.get(11).toString(),
//               System.currentTimeMillis(),
//               x.get(12).toString(),
//               System.currentTimeMillis(),
//               1,
//               System.currentTimeMillis(),
//               x.get(13).toString().toLong,
//               x.get(15).toString().toInt
//             )
//         }.toDF();
//      if(kmeansRes!=null){
//         log.info("...............................................write mongodb..................")
////        val builder = MongodbSingleton.getInstanceS()
////        kmeansRes.saveToMongodb(builder.build());
//          MongoSpark.save(kmeansRes);
//         kmeansRes.foreach { x => 
//              if(x!=null){
//                log.info("...............................................write kafka..................")
//                sendMessage(SupplyProductRes(x.get(0).toString().toInt,x.get(8).toString(),x.get(18).toString().toInt,x.get(19).toString().toInt))
//              }
//           }
//        }
//       }
//      }
//     }
//   }
//    
//    def toInt(s: String): Int = {
//      scala.util.Try(s.toInt).getOrElse(0);
//    }
//    
//    def getProducerConfig():Properties={
//      val props=new Properties()
//      props.put("metadata.broker.list","10.251.50.91:6667,10.163.123.205:6667")
//      props.put("serializer.class", classOf[IteblogEncoder[SupplyProductRes]].getName)
//      props.put("key.serializer.class", classOf[StringEncoder].getName)
//      props
//    }
//    
////    def sendMessages(messages:List[SupplyProductRes]){
////      val producer=new Producer[String,SupplyProductRes](
////          new ProducerConfig(getProducerConfig()))
////          if(messages.length>0){
////       producer.send(messages.map {
////           new KeyedMessage[String,SupplyProductRes](cRTopics,"cr",_)}:_*)
////          }
////       producer.close
////    }
//    
//     def sendMessage(message:SupplyProductRes){
//      val producer=new Producer[String,SupplyProductRes](
//          new ProducerConfig(getProducerConfig()))
//          if(message!=null){
//       producer.send(
//           new KeyedMessage[String,SupplyProductRes]("smart_sy_cr",message))
//          }
//       producer.close
//    }
//    
//    def main(args:Array[String]):Unit={
//     
//      val zookeepers=args(0).toString();
//     
//      val groupId=args(1).toString();
//     
//      val topics=args(2).toString().split(",");
//     
//      duration=toInt(args(3).toString());
//      
//      IDF_MODEL_PATH=args(4).toString();
//      
//      KMEANS_MODEL_PATH=args(5).toString();
//      
//      tfNumFeatures=toInt(args(6).toString())
//      
//      kafkaParams=Map[String,String]("zookeeper.connect" -> zookeepers, "group.id" -> groupId);
//      topics.foreach { i =>
//         kafkaTopics = Map[String, Int](i -> 1)
//       }
//      
//      streamingAnalyse()
//
//    }
//}
//
//object SQLContextSingleton{
// 
//  @transient private var instance:SQLContext=_;
//  
//  def getInstance(sparkContext:SparkContext):SQLContext={
//    if(instance==null){
//      instance=new SQLContext(sparkContext);
//    }
//    instance
//  }
//}
//object IDFModelSingleton{
//  
//  @transient private var instance:IDFModel=_;
//  
//  def getInstance(idfPath:String):IDFModel={
//    if(instance==null){
//      instance= IDFModel.load(idfPath)
//    }
//    instance
//  }
//}
//object KMEANSModelSingleton{
//  
//  @transient private var instance:KMeansModel=_;
//  
//  def getInstance(kmeansPath:String):KMeansModel={
//    if(instance==null){
//      instance=KMeansModel.load(kmeansPath)
//    }
//    instance
//  }
//}