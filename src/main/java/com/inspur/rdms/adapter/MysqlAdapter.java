package com.inspur.rdms.adapter;

import com.inspur.rdms.bean.ColumnBean;
import com.inspur.rdms.bean.TableBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.util.HashMap;
import java.util.Map;

/**
 * MySQL数据转换类型转换具体实现
 *
 * @author Neo
 * @date 2019/2/22 16:43
 */
public class MysqlAdapter extends AbstractDatabase {
	private static Logger log = LoggerFactory.getLogger(MysqlAdapter.class);
	private static final String MYSQL_DRIVER = "com.mysql.cj.jdbc.Driver";

	public MysqlAdapter(String databaseName, String host, int port, String username, String password, String jdbcType) {
		super(databaseName, host, port, username, password, jdbcType);
		log.info("Handler:MysqlAdapter");
	}

	@Override
	public String handleJdbcUrl(String jdbcType) {
		String jdbc = this.getHost() + ":" + this.getPort();
		jdbc = "jdbc:mysql://" + jdbc + "/" + this.getDatabaseName();
		jdbc += "?useUnicode=true&characterEncoding=UTF8&autoReconnect=true&failOverReadOnly=false&useSSL=false&allowMultiQueries=true";
		return jdbc;
	}

	@Override
	public String handleDriver() {
		return MYSQL_DRIVER;
	}

	@Override
	public String handleSchema(String schema) {
		return String.valueOf(this.getDatabaseName());
	}

	/**
	 * java类型与MySQL数据类型对应关系
	 *
	 * @return
	 */
	@Override
	public Map<String, String> handleFormat() {
		Map<String, String> resultMap = new HashMap<>();

		/**数值类型处理*/
		resultMap.put("int", "int");
		resultMap.put("integer", "number");
		resultMap.put("double", "double");
		resultMap.put("float", "float");

		/**日期类型处理*/
		resultMap.put("date", "date");
		resultMap.put("datetime", "datetime");
		resultMap.put("timestamp", "timestamp");

		/**二进制类型处理*/
		resultMap.put("blob", "blob");

		/**字符串处理*/
		resultMap.put("char", "char");
		resultMap.put("varchar", "varchar");

		return resultMap;
	}

	@Override
	public void handleRemarks(Connection conn) {

	}

	/**
	 * 获取建表语句
	 *
	 * @param tableBean
	 * @return
	 */
	@Override
	public String exportSql(TableBean tableBean) {

		StringBuffer sb = new StringBuffer();

		if ("TABLE".equalsIgnoreCase(tableBean.getType())) {
			sb.append("CREATE TABLE IF NOT EXISTS  `" + tableBean.getTableName() + "` (");
		} else if ("VIEW".equalsIgnoreCase(tableBean.getType())) {
			sb.append("CREATE VIEW  `" + tableBean.getTableName() + "` (");
		}
		//动态拼接字段
		for (ColumnBean column : tableBean.getColumnList()) {
			String columnType = java2db(column.getColumnTypeName().toUpperCase());
			sb.append("`" + column.getColumnName() + "` ");
			if ((column.getColumnSize() > 0) && (column.getDecimalDigits() > 0)) {
				sb.append(columnType + "(" + column.getColumnSize() + "," + column.getDecimalDigits() + ") ");
			} else if (column.getColumnSize() > 0) {
				if (column.getColumnSize() > 65535) {
					sb.append(" TEXT ");
				} else if (columnType.contains("INT") || columnType.contains("CHAR")) {
					sb.append(columnType + "(" + column.getColumnSize() + ") ");
				} else {
					sb.append(columnType + " ");
				}
			}

			if (column.getIsPk()) {
				sb.append("NOT NULL AUTO_INCREMENT PRIMARY KEY ");
			} else {
				sb.append(column.getIsNullable() ? "NULL " : "NOT NULL ");
			}
			if (!"".equals(column.getDefaultValue())) {
				sb.append(" DEFAULT '" + column.getDefaultValue().trim() + "' ");
			}

			if (!"".equals(column.getRemark())) {
				sb.append("COMMENT '" + column.getRemark() + "'");
			}
			sb.append(",");
		}

		sb = new StringBuffer(sb.toString().substring(0, sb.toString().length() - 1));
		sb.append(") ENGINE=InnoDB  DEFAULT CHARSET=utf8 ");
		return sb.toString();
	}

	@Override
	public String java2db(String dataType) {
		switch (dataType) {
			case "BIGINT":
				return "BIGINT";
			case "BINARY":
				return "TINYBLOB";
			case "BIT":
				return "BIT";
			case "CHAR":
				return "CHAR";
			case "DATE":
				return "DATE";
			case "DECIMAL":
				return "DECIMAL";
			case "NUMERIC":
			case "DOUBLE":
				return "DOUBLE";
			case "INT4":
			case "INTEGER":
				return "INT";
			case "BLOB":
				return "MEDIUMBLOB";
			case "REAL":
				return "FLOAT";
			case "SMALLINT":
				return "SMALLINT";
			case "TIME":
				return "TIME";
			case "TIMESTAMP":
				return "TIMESTAMP";
			case "TINYINT":
				return "TINYINT";
			case "VARBINAY":
				return "VARBINAY";
			case "VARCHAR":
				return "VARCHAR";
			case "LONGVARCHAR":
				return "TEXT";
			default:
				return dataType;
		}
	}

	@Override
	public Boolean createTableFromSource(TableBean tableBean) {
		if (isTableExists(tableBean.getTableName())) {
			log.info(tableBean.getTableName() + " 数据表已经存在");
			return false;
		}
		String sql = exportSql(tableBean);
		executeSql(sql);
		return true;
	}
}
