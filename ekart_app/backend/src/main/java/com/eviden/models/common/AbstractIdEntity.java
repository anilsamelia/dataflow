package com.eviden.models.common;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.MappedSuperclass;
import java.io.Serializable;

/**
 * @author Anil.Kumar
 * 
 * @version 1.0
 * 
 * 
 */
@MappedSuperclass
@ToString
public abstract class AbstractIdEntity implements Serializable {

	@Id
	@Getter
	@Setter
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	protected Long id;
}
