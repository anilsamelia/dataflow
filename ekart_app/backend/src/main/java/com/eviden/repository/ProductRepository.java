package com.eviden.repository;

import org.springframework.data.jpa.repository.JpaRepository;

import com.eviden.models.Product;

public interface ProductRepository extends JpaRepository<Product, Long> {
   
}