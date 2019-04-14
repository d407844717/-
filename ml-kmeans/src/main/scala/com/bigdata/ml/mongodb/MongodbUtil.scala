package com.bigdata.ml.mongodb
import com.mongodb.casbah.{WriteConcern => MongodbWriteConcern}
import com.stratio.datasource._
import com.stratio.datasource.mongodb._
import com.stratio.datasource.mongodb.schema._
import com.stratio.datasource.mongodb.writer._
import com.stratio.datasource.mongodb.config._
import com.stratio.datasource.mongodb.config.MongodbConfig._
import com.stratio.datasource.util.Config._

object MongodbSingleton{
  
  @transient private var instance:MongodbConfigBuilder=_;
  
  def getInstance():MongodbConfigBuilder={
    if(instance==null){
      instance=MongodbConfigBuilder(
 //               Map(Host -> List("115.28.184.141:9927"),
//                Map(Host -> List("10.45.16.132:9927"),   //生产
                Map(Host -> List("10.26.12.227:9927"),   //测试
                Database -> "bigdataml", 
                Collection -> "product_type_14000_16000", 
                SamplingRatio -> 1.0, 
                WriteConcern ->"normal",
                ConnectionsTime->"180000",
                ConnectTimeout->"50000"
                ))
    }
    instance
  }
  
  @transient private var instanceR:MongodbConfigBuilder=_;
  
  def getInstanceR():MongodbConfigBuilder={
    if(instanceR==null){
      instanceR=MongodbConfigBuilder(
//                Map(Host -> List("115.28.184.141:9927"),
//                Map(Host -> List("10.45.16.132:9927"),   //生产
                Map(Host -> List("10.26.12.227:9927"),    //测试
                Database -> "bigdataml", 
                Collection -> "company_relation_14000_16000", 
                SamplingRatio -> 1.0, 
                WriteConcern ->"normal",
                ConnectionsTime->"180000"
                ))
    }
    instanceR
  }
  
  
   @transient private var instanceS:MongodbConfigBuilder=_;
  
  def getInstanceS():MongodbConfigBuilder={
    if(instanceS==null){
      instanceS=MongodbConfigBuilder(
//                Map(Host -> List("115.28.184.141:9927"),
//                Map(Host -> List("10.45.16.132:9927"),   //生产
                Map(Host -> List("10.26.12.227:9927"),   //测试
                Database -> "icms", 
                Collection -> "sales_purchasing", 
                SamplingRatio -> 1.0, 
                WriteConcern ->"normal",
                ConnectionsTime->"180000",
                ConnectTimeout->"50000"
                ))
    }
    instanceS
  }
}