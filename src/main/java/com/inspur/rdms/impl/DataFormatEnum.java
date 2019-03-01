package com.inspur.rdms.impl;

/**
 * 标准格式类型
 *
 * @author Neo
 * @date 2019/2/21 17:22
 */

public enum DataFormatEnum {
	/**
	 * 数字类型处理
	 */
	/* 数字类型处理 */
	INT("int"),
	INTEGER("integer"),
	DOUBLE("double"),
	FLOAT("float"),
	NUMERIC("numeric"),

	/* 日期类型处理 */
	DATE("date"),
	DATETIME("datetime"),
	TIMESTAMP("timestamp"),

	/* 二进制类型处理 */
	BLOB("blob"),

	/* 字符串类型处理 */
	CHAR("char"),
	VARCHAR("varchar");

	private String format;

	private DataFormatEnum(String format) {
		this.format = format;
	}

	public String getFormat() {
		return format;
	}

	public void setFormat(String format) {
		this.format = format;
	}
}
