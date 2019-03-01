package com.inspur.rdms.exception;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * 自定义异常，继承RuntimeException
 *
 * @Author: Neo
 * @Date: 2019/2/22 9:06
 * @Description:
 */
@AllArgsConstructor
@Setter
@Getter
@ToString
public class DatabaseException extends RuntimeException {
	public static final long serialVersionUID = -1L;
	/**
	 * 异常码和异常信息
	 */
	private ExceptionEnum error;
	/**
	 * 异常发生的地方、方法名
	 */
	private String errorMethod;
	/**
	 * 异常详细信息
	 */
	private String errorDetail;
}
