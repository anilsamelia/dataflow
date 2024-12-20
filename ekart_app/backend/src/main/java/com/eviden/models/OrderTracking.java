package com.eviden.models;

import javax.persistence.CascadeType;
import javax.persistence.Entity;
import javax.persistence.JoinColumn;
import javax.persistence.OneToMany;
import javax.persistence.OneToOne;

import com.eviden.models.common.AbstractTimestampWithIdEntity;

import lombok.EqualsAndHashCode;
import lombok.Getter;
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
@EqualsAndHashCode
@Entity(name="order_tracking")
public class OrderTracking extends AbstractTimestampWithIdEntity  {
	
	private String currentStaus;
	
    @OneToOne(cascade = CascadeType.MERGE)
    @JoinColumn(name = "product_id", referencedColumnName = "id")
	private ProductOrder productOrder;


}
