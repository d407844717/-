package com.bigdata.ml.mongodb
import com.mongodb.spark._;
import org.apache.spark.sql.SQLContext
import com.stratio.datasource.util.Config._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import  com.bigdata.ml.record.CompanyRelation

object ProductTypeToCompanyRelation extends Serializable{

  val APP_NAME="daas_ml_producttype_companyrelation_test";
   def main(args:Array[String]){
      analyse();
  }
   def analyse():Unit={
     @transient
     val conf=new SparkConf()
            .setAppName(APP_NAME)
           .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
     conf.set("spark.mongodb.input.uri", "mongodb://10.26.12.227:9927/bigdataml.product_type_14000_16000?readPreference=primaryPreferred");
      conf.set("spark.mongodb.output.uri", "mongodb://10.26.12.227:9927/bigdataml.company_relation_14000_16000")
     @transient
      val sc=new SparkContext(conf);
      @transient
      val sqlContext: SQLContext = new SQLContext(sc)
      readMongodb(sqlContext,sc);
      sc.stop();
   }
   def readMongodb(sqlContext:SQLContext,sc:SparkContext):Unit={
 
     
      val salespurchasings=MongoSpark.load(sqlContext).toDF()
      salespurchasings.registerTempTable("producttype") 
    // val dataFrame=sqlContext.sql("SELECT company_name,direction,category  FROM producttype p GROUP BY p.company_name,p.direction,p.category").persist();
     val dataFrame=sqlContext.sql("select b.company_name,c.company_name,c.direction,b.category from (SELECT company_name,direction,category  FROM producttype p GROUP BY p.company_name,p.direction,p.category) b,(SELECT company_name,direction,category  FROM producttype p GROUP BY p.company_name,p.direction,p.category) c where b.company_name!=c.company_name and b.direction!=c.direction and b.category=c.category").persist();
   //  dataFrame.show()
      import sqlContext.implicits._; 
    val dataframe=dataFrame.map { x =>
      var directionInt=x.get(2).toString().toInt
                if(directionInt== -1){
                  directionInt=1
                }else if(directionInt== 1){
                  directionInt=0
                }else{
                  directionInt=2
                }
        CompanyRelation(x.get(0).toString(),x.get(1).toString(),0,directionInt,0,1,System.currentTimeMillis(),System.currentTimeMillis())
      }.toDF()
     MongoSpark.save(dataframe)
   }
}
    // println("最终去重以后的总和为："+dataFrame.count());
//    dataFrame.foreach { row =>
//         var companyname=row.get(0)
//         var direction=row.get(1)
//         var category=row.get(2)
//             println("start"+" "+companyname+" "+direction+"  "+category);
//          dataFrame.foreach { row1 => 
//              var companyname1=row1.get(0)
//              var direction1=row1.get(1)
//              var category1=row1.get(2)
//              println(companyname1+" "+direction1+"  "+category1);
//              if(companyname!=companyname1&&direction!=direction1&&category==category1){
//                var directionInt=direction1.toString().toInt
//                if(directionInt== -1){
//                  directionInt=1
//                }else if(directionInt== 1){
//                  directionInt=0
//                }else{
//                  directionInt=2
//                }
//                 val builderR=MongodbSingleton.getInstanceR();
//               // saveMongodb(sqlContext,sc,builderR,companyRelation)
//               //  new SaveMongodb().saveMongodb(sqlContext, sc, builder, companyRelation);
//                  import sqlContext._;
//                   println("start mongodb")
//          //         val dataframe=sqlContext.createDataFrame(List(CompanyRelation(companyname.toString(),companyname1.toString(),0,directionInt,0,1,System.currentTimeMillis().toLong,System.currentTimeMillis().toLong)))
//                   val dataframe=createDataFrame(sc.parallelize(List(CompanyRelation(companyname.toString(),companyname.toString(),0,directionInt,0,1,System.currentTimeMillis().toLong,System.currentTimeMillis().toLong))))
////                   dataFrame.saveToMongodb(builder.build())
//                   dataframe.saveToMongodb(builderR.build())
//              }
//         }
//      }
 // }
  
//}
//class SaveMongodb extends java.io.Serializable{
//   def saveMongodb(sqlContext:SQLContext,sc:SparkContext,builder:MongodbConfigBuilder,companyRelation:CompanyRelation):Unit={
//       import sqlContext._;
//      val dataFrame=createDataFrame(sc.parallelize(List(companyRelation)));
//      dataFrame.saveToMongodb(builder.build())
//  }
//}