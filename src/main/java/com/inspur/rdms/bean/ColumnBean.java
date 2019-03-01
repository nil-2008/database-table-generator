package com.inspur.rdms.bean;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * 数据表结构字段描述
 *
 * @author Neo
 * @date 2019/2/26 8:47
 */
@NoArgsConstructor
@AllArgsConstructor
@Getter
@Setter
public class ColumnBean {
	/**
	 * 是否主键
	 */
	private Boolean isPk;
	/**
	 * 列名
	 */
	private String columnName;
	/**
	 * 类型
	 */
	private int columnType;
	/**
	 * 类型名
	 */
	private String columnTypeName;
	/**
	 * 字段长度，不是所有的字段都有值
	 */
	private int columnSize;
	/**
	 * 精度
	 */
	private int decimalDigits;
	/**
	 * 是否为空
	 */
	private Boolean isNullable;
	/**
	 * 默认值
	 */
	private String defaultValue;
	/**
	 * 描述信息
	 */
	private String remark;
}