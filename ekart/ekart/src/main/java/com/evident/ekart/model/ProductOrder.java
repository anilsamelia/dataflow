package com.evident.ekart.model;

import java.util.List;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
@EqualsAndHashCode
public class ProductOrder {
	
	private Integer productOrderId;
	private String orderAddress;
	private String orderPhoneNumber;
	private List<Product> productList;

	

}
