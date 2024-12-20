package com.evident.ekart.model;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;

import java.io.Serializable;

import javax.persistence.*;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@Entity(name = "product_test")
public class Product implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -1347078663420132189L;

	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	private Long productId;
	
	@Column(name="product_name", length = 100)
	private String productName;
	
	
	@Column(name="product_price", length = 50)
	private Float price;

}
