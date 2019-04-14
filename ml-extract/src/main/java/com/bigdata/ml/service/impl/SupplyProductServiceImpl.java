package com.bigdata.ml.service.impl;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.ml.smart.model.SupplyProduct;
import com.ml.smart.repo.SupplyProductRepo;
import com.ml.smart.service.SupplyProductService;

@Service("supplyProductService")
public class SupplyProductServiceImpl implements SupplyProductService {

	@Autowired
	SupplyProductRepo supplyProductRepo;
	
	@Override
	public List<SupplyProduct> getSupplyProdict(int pageNumber, int pageSize) {
		return supplyProductRepo.getSupplyProduct(pageNumber, pageSize);
	}
}
