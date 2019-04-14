package com.bigdata.ml.repo.impl;

import java.text.ParseException;
import java.util.List;

import javax.annotation.Resource;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.query.Query;

import com.ml.smart.api.SpringDataPageable;
import com.ml.smart.model.SupplyProduct;
import com.ml.smart.repo.SupplyProductRepoTemp;

public class SupplyProductRepoImpl implements SupplyProductRepoTemp{

	@Resource(name = "mongoTemplate")
	 MongoOperations operations;
	
	@Override
	public List<SupplyProduct> getSupplyProduct(int pageNumber, int pageSize) {
		
		Page<SupplyProduct> pagelist=null;
		try{
			Query query=new Query();
			SpringDataPageable pageable = new SpringDataPageable();
			pageable.setPagenumber(pageNumber);
			pageable.setPagesize(pageSize);
			Long count = operations.count(query, SupplyProduct.class);
			List<SupplyProduct> list = operations.find(query.with(pageable), SupplyProduct.class);
			pagelist = new PageImpl<SupplyProduct>(list, pageable, count);
		}catch(Exception e){
			e.printStackTrace();
		}
		return pagelist.getContent();
	}

}
