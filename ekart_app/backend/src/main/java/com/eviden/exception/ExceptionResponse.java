package com.eviden.exception;

import lombok.*;

@Builder
@NoArgsConstructor
@AllArgsConstructor
@Getter
@Setter
public class ExceptionResponse {

	private String message;
	private Integer errorCode;
}
