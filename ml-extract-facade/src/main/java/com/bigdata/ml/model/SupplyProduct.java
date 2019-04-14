package com.bigdata.ml.model;

import org.codehaus.jackson.annotate.JsonProperty;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;


import lombok.Data;

@Document(collection = "sales_purchasing")
@Data
public class SupplyProduct implements java.io.Serializable{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Field("product_id")
	private Integer productId;

	@Field("product_name")
	private String productName;

	@Field("price")
	private String price;
	
	@Field("category")
	private String category;
    
	@Field("publish_time")
	private String publishTime;
    
	@Field("product_info")
	private String productInfo;
	
	@Field("product_contact")
	private String productContact;
	
	@Field("contact_phone")
	private String contactPhone;
	
	@Field("company_name")
	private String companyName;
	
	@Field("address")
	private String address;
	
	@Field("specification")
	private String specification;
	
	@Field("quantity")
	private String quantity;
	
	@Field("packing")
	private String packing;
	
	@Field("creator_user")
	private String creator_user;
	
	@Field("direction")
	private Integer direction;
}
