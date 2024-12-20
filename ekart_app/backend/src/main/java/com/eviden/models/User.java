package com.eviden.models;

import java.util.Collection;

import javax.persistence.Entity;


import com.eviden.models.common.AbstractTimestampWithIdEntity;
import com.eviden.util.Role;
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
@Entity(name = "user")
public class User extends AbstractTimestampWithIdEntity {

	private String name;
	private String email;
	private String password;
	private Role role;
	private String registerClient; 
	
	
	
}
