package com.evident.ekart.model;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
@EqualsAndHashCode
public class User {
	
	private Integer userId;
	private String userName;
	private String password;
	private String address;
	

}
