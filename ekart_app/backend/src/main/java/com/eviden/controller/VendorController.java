package com.eviden.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.*;
import com.eviden.models.Vendor;
import com.eviden.service.VendorService;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import java.util.List;
import java.util.Optional;

@RestController
@RequestMapping("/vendor")
@Api("vendor API management")
public class VendorController {

	@Autowired
	private VendorService vendorService;

	// Create a new vendor
	@ApiOperation(value = "Get a greeting message")
	@PostMapping
	@PreAuthorize("hasRole('VENDOR') or hasRole('ADMIN')")
	public Vendor createvendor(@RequestBody Vendor vendor) {
		return vendorService.savevendor(vendor);
	}

	// Get a vendor by ID
	@GetMapping("/{id}")
	@PreAuthorize("hasRole('VENDOR') or hasRole('ADMIN')")
	public ResponseEntity<Vendor> getvendorById(@PathVariable Long id) {
		Optional<Vendor> vendoroption = vendorService.getvendorById(id);
		Vendor vendor = vendoroption.get();
		return ResponseEntity.ok(vendor);
	}

	@PreAuthorize("hasRole('ADMIN')")
	@GetMapping
	public List<Vendor> getAllvendors() {
		return vendorService.getAllvendors();
	}

	@PreAuthorize("hasRole('VENDOR') or hasRole('ADMIN')")
	@DeleteMapping("/{id}")
	public ResponseEntity<Void> deletevendor(@PathVariable Long id) {
		vendorService.deletevendor(id);
		return ResponseEntity.ok().build();
	}

	@PreAuthorize("hasRole('VENDOR') or hasRole('ADMIN')")
	@PutMapping
	public Vendor updatevendor(@RequestBody Vendor vendor) {
		return vendorService.updatevendor(vendor);
	}

}
