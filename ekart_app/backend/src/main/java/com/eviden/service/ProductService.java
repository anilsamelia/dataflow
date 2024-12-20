package com.eviden.service;

import java.util.Date;
import java.util.List;
import java.util.Optional;
import org.springframework.data.domain.PageRequest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;

import com.eviden.models.Product;
import com.eviden.repository.ProductRepository;



@Service
public class ProductService {

    @Autowired
    private ProductRepository productRepository;

    // Save a user
    public Product saveProduct(Product product) {
    	product.setCreatedOn(new Date());
    	product.setLastModifiedAt(new Date());
        return productRepository.save(product);
    }

    // Get a user by ID
    public Optional<Product> getProductById(Long id) {
        return productRepository.findById(id);
    }

    // Get all users
    public List<Product> getAllProducts() {
        return productRepository.findAll();
    }
    
    public Page<Product> getProducts(int page, int size) {
        Pageable pageable = PageRequest.of(page, size); // Create a Pageable instance
        return productRepository.findAll(pageable); // Fetch the paginated result
    }

    // Delete a user by ID
    public void deleteProduct(Long id) {
        productRepository.deleteById(id);
    }
    

	
}
