package com.eviden.service;

import java.util.Date;
import java.util.List;
import java.util.Optional;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.eviden.exception.ServiceException;
import com.eviden.models.Customer;
import com.eviden.repository.CustomerRepository;

@Service
public class CustomerService {
    @Autowired
    private CustomerRepository customerRepository;

    // Save a Customer
    public Customer saveCustomer(Customer customer) {
    	customer.setCreatedOn(new Date());
    	customer.setLastModifiedAt(new Date());
        return customerRepository.save(customer);
    }

    // Get a Customer by ID
    public Optional<Customer> getCustomerById(Long id) {
    	Optional<Customer> customeropt = customerRepository.findById(id);
    	if (customeropt.isPresent()) {
    		return customeropt;
    	}else {
    	 throw new ServiceException("No Customer found",404);	
    	}
    }

    // Get all Customers
    public List<Customer> getAllCustomers() {
        return customerRepository.findAll();
    }

    // Delete a Customer by ID
    public void deleteCustomer(Long id) {
        customerRepository.deleteById(id);
    }
    
    public Customer updateCustomer(Customer customer) {
    	Optional<Customer> optCustomer = customerRepository.findById(customer.getId());
    	if (optCustomer.isPresent()) {
    		Customer ocustomer = optCustomer.get();
    		customer.setCreatedOn(ocustomer.getCreatedOn());
    		customer.setLastModifiedAt(new Date());
    		customerRepository.save(customer);
    	}else {
    		throw new ServiceException("Customer Not found",404);
    	}
    	return customer;
     }
}
