package com.inspur.rdms.impl;

import com.inspur.rdms.bean.ColumnBean;
import com.inspur.rdms.bean.TableBean;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Database方法接口
 *
 * @Author: Neo
 * @Date: 2019/2/21 16:10
 * @Description:
 */
public interface DatabaseImpl {
	/**
	 * 服务器是否可以连接： false：不可以 true：可以
	 */
	public boolean canConnect();

	/**
	 * 数据库是否可以连接： false：不可以 true：可以
	 */
	public boolean isDatabaseConnect();

	/**
	 * 执行sql语句
	 *
	 * @param sql
	 */
	public void executeSql(String sql);

	/**
	 * 连接数据库
	 *
	 * @return
	 */
	public Connection getConnection();

	/**
	 * 获取对应的jdbc
	 *
	 * @return String
	 */
	public String getJDBCUrl();

	/**
	 * 获取数据库所有的schema
	 *
	 * @return
	 */
	public List querySchema();

	/**
	 * 根据数据库类型获取对应的schema,如果没有，则不处理
	 *
	 * @param schema
	 * @return
	 */
	public String getSchema(String schema);

	/**
	 * 判断数据表是否存在
	 *
	 * @param tableName
	 * @return
	 */
	public boolean isTableExists(String tableName);

	/**
	 * 获取表的描述信息
	 *
	 * @param schema
	 * @param type
	 * @return
	 */
	public List<TableBean> queryTable(String schema, String type);

	/**
	 * 获取表列的描述信息
	 *
	 * @param schema    数据库的schema
	 * @param tableName 表名
	 * @return TableBean 列描述信息列表
	 */
	public ArrayList<ColumnBean> queryTableColumns(String schema, String tableName);

	/**
	 * 获取表的索引和统计信息描述
	 *
	 * @param schema
	 * @param tableName
	 * @return
	 */
	public List queryTableIndex(String schema, String tableName);

	/**
	 * 主键列描述信息
	 *
	 * @param schema
	 * @param tableName
	 * @return
	 */
	public List queryTablePrimaryKey(String schema, String tableName);

	/**
	 * 获取driver
	 *
	 * @return
	 */
	public String getDriverClass();

	/**
	 * 释放资源
	 *
	 * @param conn
	 * @param psmt
	 * @param rs
	 */
	public void closeObject(Connection conn, PreparedStatement psmt, ResultSet rs);

	/**
	 * handleJdbcUrl
	 *
	 * @param jdbcType 数据库类别
	 * @return String 连接的URL
	 */
	public String handleJdbcUrl(String jdbcType);

	/**
	 * handleDriver
	 *
	 * @return String handleDriver
	 */
	public String handleDriver();

	/**
	 * handleSchema
	 *
	 * @param schema 模式
	 * @return String handleSchema
	 */
	public String handleSchema(String schema);

	/**
	 * 数据库对应
	 *
	 * @return Map handleFormat
	 */
	public Map<String, String> handleFormat();

	/**
	 * 描述
	 *
	 * @param conn Connection
	 */
	public void handleRemarks(Connection conn);

	/**
	 * 返回原始表数据结构
	 *
	 * @param schema
	 * @param tableName
	 * @return
	 */
	public TableBean getSourceTableColumns(String schema, String tableName);

	/**
	 * 导出SQL语句
	 *
	 * @param tableBean
	 * @return
	 */
	public String exportSql(TableBean tableBean);

	/**
	 * Java到数据库类型转换
	 *
	 * @param dataType
	 * @return
	 */
	public String java2db(String dataType);

	/**
	 * 动态创建目标表
	 *
	 * @param tableBean
	 * @return true:创建成功 false:创建失败
	 */
	public Boolean createTableFromSource(TableBean tableBean);

}
