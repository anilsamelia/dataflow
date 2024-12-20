package com.eviden.ekart.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import com.evident.ekart.model.Product;


public interface ProductRepository extends JpaRepository<Product, Long> {

}
