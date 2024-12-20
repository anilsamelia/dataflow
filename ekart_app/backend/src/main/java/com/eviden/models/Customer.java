package com.eviden.models;


import com.eviden.models.common.AbstractTimestampWithIdEntity;

import javax.persistence.CascadeType;
import javax.persistence.Entity;
import javax.persistence.JoinColumn;
import javax.persistence.OneToOne;

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
@Entity(name = "customer")
public class Customer extends AbstractTimestampWithIdEntity  {
	
	private String firstName;
	private String lastName;
	private String address;
	private String phoneNumber;
	
    @OneToOne(cascade = CascadeType.MERGE)
    @JoinColumn(name = "user_id", referencedColumnName = "id")
	private User user;
}
