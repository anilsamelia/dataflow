package com.eviden.util;

public enum Role {

	ROLE_ADMIN("ROLE_ADMIN"), ROLE_CUSTOMER("ROLE_CUSTOMER"), ROLE_VENDOR("ROLE_VENDOR");

	private final String value;

	// Constructor
	Role(String value) {
		this.value = value;
	}

	// Getter method for the description
	public String getDescription() {
		return value;
	}

}
