package com.evident.ekart.model;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
@EqualsAndHashCode
public class OrderTracking {
	
	private Integer orderTrackingId;
	private ProductOrder productOrder;


}
