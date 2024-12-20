package com.eviden.ekart.controller;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RestController;
import com.eviden.ekart.service.ProductService;


@SuppressWarnings("hiding")
@RestController
public class ProductController<Product> {
	
	@Autowired
	private ProductService productService;
	

	@GetMapping
	public List<com.evident.ekart.model.Product> listOfProduct(){
		return productService.listOfProduct();
	}
	
	@PutMapping
	public Product updateProduct(Product product) {
		return null;
	}
	
	@PostMapping
	public Product saveProduct(Product product) {
		return null;
	}

	@DeleteMapping
	public ResponseEntity<Product> deleteProduct(Product product) {
		return null;
	}
	
	
}
