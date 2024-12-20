package com.eviden.ekart.service;

import java.util.Arrays;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.eviden.ekart.repository.ProductRepository;
import com.evident.ekart.model.Product;

@Service
public class ProductService {
	
	@Autowired
	ProductRepository productRepository;
	
	
	public List<Product> listOfProduct(){
		//List<Product> list =Arrays.asList(new Product(1, "HEAD Phone",4000f),new Product(2, "Smart Watch",1999f), new Product(3,"PS5 game",34000f));
		
		return productRepository.findAll();
	}
	
	
	public Product save(Product product) {
		return productRepository.save(product);
	}

	
	
}
