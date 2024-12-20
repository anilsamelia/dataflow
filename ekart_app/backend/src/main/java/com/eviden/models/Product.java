package com.eviden.models;

import java.util.Date;

import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.JoinColumn;
import javax.persistence.OneToOne;

import com.eviden.models.common.AbstractTimestampWithIdEntity;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

/**
 * @author Anil.Kumar
 * 
 * @version 1.0
 * 
 * 
 */
@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@ToString
@Entity(name = "product")
public class Product extends AbstractTimestampWithIdEntity {
	
	
	@Column(name = "product_name", nullable = false)
	private String productName;

	private String description;
	
	@Column(name = "price", nullable = false)
	private Float price;

	@Column(name = "stock_qty",nullable = false)
	private Integer stockQty;

	private String category;

	@Column(name = "image_url",nullable = false)
	private String imageUrl;

	@Column(name = "expiry_date")
	private Date expiryDate;

	@OneToOne(cascade = CascadeType.MERGE)
	@JoinColumn(name = "vendor_id", referencedColumnName = "id", nullable = false)
	private Vendor vendor;

}
