package com.inspur.rdms.exception;

/**
 * @Author: Neo
 * @Date: 2019/2/22 9:11
 * @Description: 枚举异常处理类
 */
public enum ExceptionEnum {
	/**
	 * 常见异常信息定义
	 */
	ADDRESS_CONNECT_EXCEPTION(1001, "地址连接异常"),

	DATABASE_CONNECT_EXCEPTION(1002, "数据库地址连接异常"),

	DATASOURCE_CONNECT_EXCEPTION(1003, "获取数据源连接异常"),

	DATABASE_STRUCTURE_EXCEPTION(1004, "查询数据库结构异常"),

	DATABASE_QUERY_EXCEPTION(1005, "查询数据结果集异常"),

	SQL_EXECUTE_EXCEPTION(1006, "SQL语句执行异常"),

	CLOSE_CONNECT_EXCEPTION(1007, "关闭连接异常"),

	HANDLER_CLASS_EXCEPTION(1008, "HANDLER子类处理异常"),

	OTHER_UNKNOWN_EXCEPTION(1009, "其他异常");

	private int errorCode;
	private String errorMessage;

	public int getErrorCode() {
		return errorCode;
	}

	public void setErrorCode(int errorCode) {
		this.errorCode = errorCode;
	}

	public String getErrorMessage() {
		return errorMessage;
	}

	public void setErrorMessage(String errorMessage) {
		this.errorMessage = errorMessage;
	}

	/**
	 * @param errorCode
	 * @param errorMessage
	 */
	private ExceptionEnum(int errorCode, String errorMessage) {
		this.errorCode = errorCode;
		this.errorMessage = errorMessage;
	}
}
