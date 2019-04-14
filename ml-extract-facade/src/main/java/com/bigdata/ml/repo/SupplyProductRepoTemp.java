package com.bigdata.ml.repo;

import java.util.List;

import org.springframework.data.mongodb.repository.MongoRepository;

import com.bigdata.ml.model.SupplyProduct;

public interface SupplyProductRepoTemp{
   public List<SupplyProduct> getSupplyProduct(int pageNumber,int pageSize);
}
