package com.bigdata.ml.service;

import java.util.List;

import com.bigdata.ml.model.SupplyProduct;

public interface SupplyProductService {
   public List<SupplyProduct> getSupplyProdict(int pageNumber,int pageSize);
}
