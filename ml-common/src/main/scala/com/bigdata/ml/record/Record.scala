package com.bigdata.ml.record

case class ProductType(
                   product_id:Int,
                   company_name:String,
                   direction:Int,
                   category:Long
                )
case class SupplyProductRes(
                   product_id:Long,
                   company_name:String,                   
                   direction:Long,
                   category:Long
                )
case class SupplyProductKafka(
                   product_id:Int,
                   company_name:String,
                   product_info:String,
                   direction:Int
                 )
case class CompanyRelation(
                   company_name:String,
                   rel_company_name:String,
                   data_origin:Int,
                   orientation:Int,
                   customer_type:Int,
                   status:Int,
                   create_time:Long,
                   modify_time:Long
                 )
case class SalesPurchasingKafka(
                   product_id:Long,
                   product_name:String,
                   price:String,
                   category:String,
                   product_info:String,
                   product_contacts:String,
                   contact_phone:String,
                   company_name:String,
                   address:String,
                   specification:String,
                   quantity:String,
                   packing:String,
                   creator_user:String,
                   direction:Long,
                   product_infoik:String
                 )
case class SalesPurchasingPreRes(
                   product_id:Long,
                   product_name:String,
                   price:String,
                   category:String,
                   publish_time:String,
                   product_info:String,
                   product_contacts:String,
                   contact_phone:String,
                   company_name:String,
                   address:String,
                   specification:String,
                   quantity:String,
                   packing:String,
                   create_time:Long,
                   creator_user:String,
                   last_modify:Long,
                   is_complete:Long,
                   complete_time:Long,
                   direction:Long
                 )
case class SalesPurchasingToRelation(
                   product_id:Long,
                   product_name:String,
                   product_info:String,
                   company_name:String,
                   direction:Long,
                   product_infoik:String
                 )
case class SalesPurchasingRes(
                   product_id:Long,
                   product_name:String,
                   price:String,
                   category:String,
                   publish_time:String,
                   product_info:String,
                   product_contacts:String,
                   contact_phone:String,
                   company_name:String,
                   address:String,
                   specification:String,
                   quantity:String,
                   packing:String,
                   create_time:Long,
                   creator_user:String,
                   last_modify:Long,
                   is_complete:Long,
                   complete_time:Long,
                   direction:Long,
                   category_cal:Long
                 )
case class Record(
                  id: String, 
                  companyname: String,
                  direction: String,
                  productinfo: String
                 )
case class SalesPurchasingRelation(
                  product_id:Long,
                  rel_product_id:Long,
                  product_name:String,
                  rel_product_name:String,
                  company_name:String,
                  rel_company_name:String,
                  rel_product_info:String,
                  direction:Long,
                  category_cal:Long,
                  create_time:Long,
                  modify_time:Long
                 )