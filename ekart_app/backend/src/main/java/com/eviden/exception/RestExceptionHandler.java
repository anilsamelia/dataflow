package com.eviden.exception;


import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;

@ControllerAdvice
@Slf4j
public class RestExceptionHandler {

	@ExceptionHandler(ServiceException.class)
	public ResponseEntity<ExceptionResponse> exception(ServiceException ex) {
		log.error("Unexpected service error", ex);
		ExceptionResponse exceptionResponse = ExceptionResponse.builder().message(ex.getMessage()).errorCode(ex.getErrorCode()).build();
		return ResponseEntity.status(500).body(exceptionResponse);
	}
}
