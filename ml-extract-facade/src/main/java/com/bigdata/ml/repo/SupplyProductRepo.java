package com.bigdata.ml.repo;

import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

import com.bigdata.ml.model.SupplyProduct;

@Repository("supplyProductRepo")
public interface SupplyProductRepo extends MongoRepository<SupplyProduct, String>,SupplyProductRepoTemp{

}
