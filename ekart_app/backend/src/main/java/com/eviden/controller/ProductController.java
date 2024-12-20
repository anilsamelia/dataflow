package com.eviden.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.*;
import com.eviden.models.Product;
import com.eviden.service.ProductService;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Path;
import java.util.List;
import java.util.Properties;
import java.nio.file.Path;
import java.nio.file.Paths;

@RestController
@RequestMapping("/product")
public class ProductController {

	@Autowired
	private ProductService productService;
	

	@PostMapping
	//@PreAuthorize("hasRole('VENDOR') or hasRole('ADMIN')")
	public Product createProduct(@RequestBody Product product) {
		return productService.saveProduct(product);
	}

	@PutMapping
	//@PreAuthorize("hasRole('VENDOR') or hasRole('ADMIN')")
	public Product updateProduct(@RequestBody Product product) {
		return productService.saveProduct(product);
	}

	@GetMapping
	public Page<Product> getlist(@RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "10") int size) {
		getFiles() ;
		return productService.getProducts(page,size);
	}

	@GetMapping("/{id}")
	public Product getProductById(@PathVariable Long id) {
		return productService.getProductById(id).get();
	}

	// Delete a user by ID
	@DeleteMapping("/{id}")
	@PreAuthorize("hasRole('VENDOR') or hasRole('ADMIN')")
	public ResponseEntity<Void> deleteProduct(@PathVariable Long id) {
		return ResponseEntity.ok().build();
	}

	public String getFiles() {
		Properties prop = new Properties();
		URL fileUrl= ProductController.class.getClassLoader().getResource("myfile.properties");//getResourceAsStream("myfile.properties");
		  if (fileUrl != null) {
	            // Convert URL to Path
	            Path filePath = Paths.get(fileUrl.getPath());
	            System.out.println("File path: " + filePath);
	        } 
	     return "";
	}
}
