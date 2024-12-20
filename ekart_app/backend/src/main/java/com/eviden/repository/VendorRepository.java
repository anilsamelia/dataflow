package com.eviden.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import com.eviden.models.Vendor;

public interface VendorRepository extends JpaRepository<Vendor, Long> {

}
