package com.bigdata.ml.idfkmeans

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.sql.{Row, SQLContext};
import org.apache.spark.ml.feature.IDFModel
import org.apache.spark.sql.SQLContext;
import org.apache.spark.ml.clustering.KMeans
import com.bigdata.ml.record.ProductType;
import com.bigdata.ml.record.Record;
import com.bigdata.ml.mongodb.MongodbSingleton;
import com.mongodb.casbah.{WriteConcern => MongodbWriteConcern}
import com.stratio.datasource._
import com.stratio.datasource.mongodb._
import com.stratio.datasource.mongodb.schema._
import com.stratio.datasource.mongodb.writer._
import com.stratio.datasource.mongodb.config._
import com.stratio.datasource.mongodb.config.MongodbConfig._
import com.stratio.datasource.util.Config._
import org.apache.commons.cli._
import org.slf4j.LoggerFactory

object SalesPurchasingKmeans {
  val log = LoggerFactory.getLogger(SalesPurchasingKmeans.getClass)
  
  val APP_NAME = "daas_ml_producttfidf_kmeans"
  
    var tfNumFeatures =5000
     var DATA_INPUT_PATH = "hdfs://10.174.229.47:8020/ml/data/segmentData/"
    var MODEL_PATH_IDF="hdfs://10.174.229.47:8020/ml/test/model/idfmodel/idfmodel3000_5000"
    var MODEL_PATH_KMEANS="hdfs://10.174.229.47:8020/ml/test/model/kmeansmodel/kmeansmodel3000_5000"
   //分类个数
   var numClusters=3000;
   //迭代次数
   var numlterations=20;
  def analyse(): Unit = {

    val conf = new SparkConf()
        .setAppName(APP_NAME)
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val sc = new SparkContext(conf)

    tfidfProcess(sc)

    // shutdown spark context
    sc.stop()
  }

  private[SalesPurchasingKmeans] def tfidfProcess(sc: SparkContext): Unit = {
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
   

    val idfModel= new IDF()
        .setInputCol("productFeatures")
        .setOutputCol("features")
        .fit(tfData);
    //保存IDFModel
    idfModel.save(MODEL_PATH_IDF);
    //计算idf
    val idfData=idfModel.transform(tfData)

     val trainingData = idfData.select($"id", $"companyname",$"direction",$"features").cache()
   //  var broadcastData=sc.broadcast(trainingData)
    val kmeans=new KMeans()
       .setK(numClusters)
       .setMaxIter(numlterations)
       .setFeaturesCol("features")
       .setPredictionCol("prediction")
       .setInitSteps(2)
     
    val kmeansmodel=kmeans.fit(trainingData)
   
    kmeansmodel.save(MODEL_PATH_KMEANS)
//    
//    val kmeansData=kmeansmodel.transform(trainingData).cache()
//    
//    val result=kmeansData.select($"id", $"companyname",$"direction",$"prediction").map { x => 
//       ProductType(x.get(0).toString().toInt,x.get(1).toString(),x.get(2).toString().toInt,x.get(3).toString().toInt)
//      }.toDF()
//      //result.show(100)
//      val builder = MongodbSingleton.getInstance()
//      result.saveToMongodb(builder.build());
  }

  def main(args: Array[String]) {
     //向量维度
      tfNumFeatures = args(0).toInt;
      //原数据获取地址
      DATA_INPUT_PATH=args(1).toString();
      //idf模型存储地址
      MODEL_PATH_IDF=args(2).toString();
      //kemans地址存储
      MODEL_PATH_KMEANS=args(3).toString();
      //分类个数
      numClusters=args(4).toInt;
      //迭代次数
      numlterations=args(5).toInt;
      analyse;
  }
   def toInt(s: String): Int = {
      scala.util.Try(s.toInt).getOrElse(0);
    }
}