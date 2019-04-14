package com.bigdata.ml.serialize
import kafka.serializer.{Decoder, StringDecoder,Encoder}
import kafka.utils.VerifiableProperties
import java.io.{ByteArrayInputStream,ObjectInputStream,ByteArrayOutputStream,ObjectOutputStream}
import org.apache.avro.io.{DatumReader, DecoderFactory}
import org.apache.avro.specific.SpecificDatumReader
import com.smart.ml.entity.{SalesPurchasing,SupplyProduct}
import org.apache.spark.{SparkException}

class IteblogDecoder[T](props:VerifiableProperties=null) extends Decoder[T]{
 
  override def fromBytes(bytes:Array[Byte]):T={
    var t:T=null.asInstanceOf[T]
    var bi:ByteArrayInputStream=null
    var oi:ObjectInputStream=null
    try{
      bi=new ByteArrayInputStream(bytes)
      oi=new ObjectInputStream(bi)
      t=oi.readObject().asInstanceOf[T]
    }catch{
      case e:Exception=>{
        e.printStackTrace();null
      }
    }finally{
      bi.close()
      oi.close()
    }
    t
  }
}

class IteblogEncoder[T](props:VerifiableProperties=null) extends Encoder[T]{
  
  override def toBytes(t:T):Array[Byte]={
    if(t==null)
      null
    else{
      var bo:ByteArrayOutputStream=null
      var oo:ObjectOutputStream=null
      var byte:Array[Byte]=null
      try{
        bo=new ByteArrayOutputStream()
        oo=new ObjectOutputStream(bo)
        oo.writeObject(t)
        byte=bo.toByteArray()
      }catch{
        case ex:Exception=>return byte
      }finally{
        bo.close()
        oo.close()
      }
      byte
    }
  }
}

class SupplyProductDecoder(props: VerifiableProperties = null) extends Decoder[SupplyProduct] {

  val read: DatumReader[SupplyProduct] = new SpecificDatumReader[SupplyProduct](SupplyProduct.getClassSchema)

  override def fromBytes(bytes: Array[Byte]): SupplyProduct = {
    if (bytes == null || bytes.length == 0) {
      throw new SparkException("Message is none.")
    }

    val decoder = DecoderFactory.get().binaryDecoder(bytes, null)
    read.read(null, decoder)
  }
}

class SalesPurchasingDecoder(props: VerifiableProperties = null) extends Decoder[SalesPurchasing] {

  val read: DatumReader[SalesPurchasing] = new SpecificDatumReader[SalesPurchasing](SalesPurchasing.getClassSchema)

  override def fromBytes(bytes: Array[Byte]): SalesPurchasing = {
    if (bytes == null || bytes.length == 0) {
      throw new SparkException("Message is none.")
    }

    val decoder = DecoderFactory.get().binaryDecoder(bytes, null)
    read.read(null, decoder)
  }
}