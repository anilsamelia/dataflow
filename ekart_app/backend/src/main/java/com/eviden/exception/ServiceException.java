package com.eviden.exception;


import lombok.Getter;


//@ResponseStatus(value = HttpStatus.BAD_REQUEST, reason = "Error message")

@Getter
public class ServiceException extends RuntimeException {

	private int errorCode = 500;

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public ServiceException(String message, Throwable e) {
		super(message, e);
	}



	public ServiceException(String message, int errorcode) {
		super(message);
		this.errorCode = errorcode;

	}
}
