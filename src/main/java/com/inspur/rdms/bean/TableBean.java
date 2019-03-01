package com.inspur.rdms.bean;

/**
 * @author Neo
 * @date 2019/2/26 10:50
 */

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.List;

/**
 * 表基本结构
 *
 * @author Neo
 * @date 2019/2/26 8:46
 */
@NoArgsConstructor
@AllArgsConstructor
@Getter
@Setter
public class TableBean {
	/**
	 * 表名
	 */
	private String tableName;
	/**
	 * 数据表字段列表
	 */
	private List<ColumnBean> columnList;
	/**
	 * 表的类型：TABLE,VIEW
	 */
	private String type;
	/**
	 * 数据表描述
	 */
	private String remark;
}

