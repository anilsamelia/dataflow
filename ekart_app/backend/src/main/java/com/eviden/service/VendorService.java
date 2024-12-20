package com.eviden.service;

import java.util.Date;
import java.util.List;
import java.util.Optional;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.stereotype.Service;

import com.eviden.exception.ServiceException;
import com.eviden.models.Vendor;
import com.eviden.repository.VendorRepository;

@Service
public class VendorService {
	
    @Autowired
    private VendorRepository vendorRepository;

    @PreAuthorize("hasRole('VENDOR')")
    public Vendor savevendor(Vendor vendor) {
    	vendor.setCreatedOn(new Date());
    	vendor.setLastModifiedAt(new Date());
        return vendorRepository.save(vendor);
    }

    // Get a vendor by ID
    @PreAuthorize("hasRole('VENDOR')")
    public Optional<Vendor> getvendorById(Long id) {
    	Optional<Vendor> vendoropt = vendorRepository.findById(id);
    	if (vendoropt.isPresent()) {
    		return vendoropt;
    	}else {
    	 throw new ServiceException("No vendor found",404);	
    	}
    }

    // Get all vendors
    @PreAuthorize("hasRole('VENDOR')")
    public List<Vendor> getAllvendors() {
        return vendorRepository.findAll();
    }

    // Delete a vendor by ID
    @PreAuthorize("hasRole('VENDOR')")
    public void deletevendor(Long id) {
        vendorRepository.deleteById(id);
    }
    
    @PreAuthorize("hasRole('VENDOR')")
    public Vendor updatevendor(Vendor vendor) {
    	Optional<Vendor> optvendor = vendorRepository.findById(vendor.getId());
    	if (optvendor.isPresent()) {
    		Vendor ovendor = optvendor.get();
    		vendor.setCreatedOn(ovendor.getCreatedOn());
    		vendor.setLastModifiedAt(new Date());
    		vendorRepository.save(vendor);
    	}else {
    		throw new ServiceException("vendor Not found", 404);
    	}
    	return vendor;
     }
}
