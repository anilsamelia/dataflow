package com.eviden.models;

import java.util.List;

import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.OneToMany;
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
@ToString
@Entity(name ="product_order")
public class ProductOrder extends AbstractTimestampWithIdEntity {
	
	
		
	@Column(name="order_phone_number")
	private String orderPhoneNumber;
	
	@Column(name="shipping_address")
	private String shippingAddress;
	
    @OneToMany(cascade = CascadeType.MERGE)
    @JoinColumn(name = "product_id")
	private List<Product> productList;
	
    @OneToOne(cascade = CascadeType.MERGE)
    @JoinColumn(name = "customer_id", referencedColumnName = "id")
	private Customer customer;

	

}
