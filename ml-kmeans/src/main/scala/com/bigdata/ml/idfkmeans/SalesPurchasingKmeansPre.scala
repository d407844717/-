package com.bigdata.ml.idfkmeans

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.sql.{Row, SQLContext};
import org.apache.spark.ml.feature.IDFModel
import org.apache.spark.ml.clustering.KMeansModel
import org.apache.spark.sql.SQLContext;
import org.apache.spark.ml.clustering.KMeans
import com.bigdata.ml.record.ProductType;
import com.bigdata.ml.record.Record;
import com.mongodb.spark._;

object SalesPurchasingKmeansPre {
  
  val APP_NAME = "daas_ml_producttfidf_kmeans_result"
  
   var DATA_INPUT_PATH = "hdfs://10.174.229.47:8020/ml/data/segmentData/"
    var MODEL_PATH_IDF="hdfs://10.174.229.47:8020/ml/test/model/idfmodel/idfmodel14000_16000"
    var MODEL_PATH_KMEANS="hdfs://10.174.229.47:8020/ml/test/model/kmeansmodel/kmeansmodel14000_16000"
  var tfNumFeatures =16000
  
  def analyse(): Unit = {

    val conf = new SparkConf()
        .setAppName(APP_NAME)
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
         conf.set("spark.mongodb.output.uri", "mongodb://10.26.12.227:9927/icms.product_type_14000_16000")

    val sc = new SparkContext(conf)

    tfidfProcess(sc)

    // shutdown spark context
    sc.stop()
  }

  private[SalesPurchasingKmeansPre] def tfidfProcess(sc: SparkContext): Unit = {
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._;

    val records = sc.textFile(DATA_INPUT_PATH).map{  x =>
        val data = x.split(",")
        Record(data(0), data(1), data(2),data(3))     
    }.toDF.cache()

    val wordsData = new Tokenizer()
        .setInputCol("productinfo")
        .setOutputCol("productwords")
        .transform(records)

    val tfData = new HashingTF()
        .setNumFeatures(tfNumFeatures)
        .setInputCol("productwords")
        .setOutputCol("productFeatures")
        .transform(wordsData)
   

    val idfModel=IDFModelSingleton.getInstance(MODEL_PATH_IDF)
    //计算idf
    val idfData=idfModel.transform(tfData)

     val trainingData = idfData.select($"id", $"companyname",$"direction",$"features")

     
    val kmeansmodel=KMEANSModelSingleton.getInstance(MODEL_PATH_KMEANS)
   
    
    val kmeansData=kmeansmodel.transform(trainingData).cache()
    
    val result=kmeansData.select($"id", $"companyname",$"direction",$"prediction").map { x => 
       ProductType(x.get(0).toString().toInt,x.get(1).toString(),x.get(2).toString().toInt,x.get(3).toString().toLong)
      }.toDF()
      //result.show(100)
     MongoSpark.save(result);
  }

  def main(args: Array[String]) {
     tfNumFeatures = args(0).toInt;
    analyse;

  }
  def toInt(s: String): Int = {
      scala.util.Try(s.toInt).getOrElse(0);
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