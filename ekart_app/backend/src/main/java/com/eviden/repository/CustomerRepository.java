package com.eviden.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import com.eviden.models.Customer;

public interface CustomerRepository extends JpaRepository<Customer, Long> {

}
