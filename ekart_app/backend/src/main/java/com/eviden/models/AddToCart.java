package com.eviden.models;

import java.util.List;

import javax.persistence.CascadeType;
import javax.persistence.Entity;
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
@AllArgsConstructor
@NoArgsConstructor
@ToString
@Entity(name = "add_to_cart")
public class AddToCart extends AbstractTimestampWithIdEntity {

	/*
	 * @OneToMany(cascade = CascadeType.MERGE)
	 * 
	 * @JoinColumn(name = "product_id") private List<Product> list;
	 * 
	 * @OneToOne(cascade = CascadeType.MERGE)
	 * 
	 * @JoinColumn(name = "user_id") private List<User> user;
	 */
    
    private Boolean status;

}
