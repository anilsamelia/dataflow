package com.eviden.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.*;
import com.eviden.models.Customer;
import com.eviden.service.CustomerService;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import java.util.List;
import java.util.Optional;

@RestController
@RequestMapping("/customer")
@Api("Customer API management")
public class CustomerController {

    @Autowired
    private CustomerService customerService;
    

    @PostMapping
      public Customer createCustomer(@RequestBody Customer customer) {
        return customerService.saveCustomer(customer);
    }

    // Get a customer by ID
    @GetMapping("/{id}")
    @PreAuthorize("hasRole('CUSTOMER') or hasRole('ADMIN')")
    public ResponseEntity<Customer> getCustomerById(@PathVariable Long id) {
        Optional<Customer> customeroption = customerService.getCustomerById(id);
        Customer customer = customeroption.get();
        return ResponseEntity.ok(customer);
    }

    // Get all customers
    @GetMapping
    @PreAuthorize("hasRole('ADMIN')")
    public List<Customer> getAllCustomers() {
        return customerService.getAllCustomers();
    }

    // Delete a customer by ID
    @DeleteMapping("/{id}")
    @PreAuthorize("hasRole('CUSTOMER') or hasRole('ADMIN')")
    public ResponseEntity<Void> deleteCustomer(@PathVariable Long id) {
        customerService.deleteCustomer(id);
        return ResponseEntity.ok().build();
    }
    
    // update a existing customer
    @PutMapping
    @PreAuthorize("hasRole('CUSTOMER') or hasRole('ADMIN')")
      public Customer updateCustomer(@RequestBody Customer customer) {
        return customerService.updateCustomer(customer);
    }


}

