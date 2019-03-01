package com.inspur.rdms.adapter;

import com.inspur.rdms.bean.ColumnBean;
import com.inspur.rdms.bean.TableBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.util.Map;

/**
 * PostgreSQL数据转换类型转换具体实现
 *
 * @author Neo
 * @date 2019/2/22 17:31
 */
public class PostgresqlAdapter extends AbstractDatabase {
	private static Logger log = LoggerFactory.getLogger(PostgresqlAdapter.class);
	private static final String POSTGRESQL_DRIVER = "org.postgresql.Driver";

	public PostgresqlAdapter(String databaseName, String host, int port, String username, String password, String jdbcType) {
		super(databaseName, host, port, username, password, jdbcType);
		log.info("Handler:PostgresqlAdapter");
	}

	@Override
	public String handleJdbcUrl(String jdbcType) {
		String jdbc = this.getHost() + ":" + this.getPort();
		jdbc = "jdbc:postgresql://" + jdbc;
		if (this.getDatabaseName() != null) {
			jdbc += "/" + this.getDatabaseName();
		}
		return jdbc;
	}

	@Override
	public String handleDriver() {
		return POSTGRESQL_DRIVER;
	}

	@Override
	public String handleSchema(String schema) {
		// postgres的schema暂时都为public(默认)
		if (null == schema || "".equals(schema)) {
			schema = "public";
		}
		return schema;
	}

	@Override
	public Map<String, String> handleFormat() {
		return null;
	}

	@Override
	public void handleRemarks(Connection conn) {

	}

	/**
	 * java.sql.type2
	 *
	 * @param type
	 * @return
	 */
	@Override
	public String getTypeById(int type) {
		switch (type) {
			case -7:
				return "BIT";
			case -6:
				return "SMALLINT";
			case 5:
				return "SMALLINT";
			case 4:
				return "INT4";
			case -5:
				return "INT8";
			case 6:
				return "NUMERIC";
			case 7:
				return "NUMERIC";
			case 8:
				return "NUMERIC";
			case 2:
				return "NUMERIC";
			case 3:
				return "DECIMAL";
			case 1:
				return "VARCHAR";
			case 12:
				return "VARCHAR";
			case -1:
				return "TEXT";
			case 91:
				return "DATE";
			case 92:
				return "TIME";
			case 93:
				return "TIMESTAMP";
			case -2:
				return "BINARY";
			case -3:
				return "VARBINARY";
			case -4:
				return "LONGVARBINARY";
			case 0:
				return "NULL";
			case 1111:
				return "OTHER";
			case 2000:
				return "JAVA_OBJECT";
			case 2001:
				return "DISTINCT";
			case 2002:
				return "STRUCT";
			case 2003:
				return "ARRAY";
			case 2004:
				return "BLOB";
			case 2005:
				return "CLOB";
			case 2006:
				return "REF";
			case 70:
				return "DATALINK";
			case 16:
				return "BOOLEAN";
			case -8:
				return "ROWID";
			case -15:
				return "NCHAR";
			case -9:
				return "NVARCHAR";
			case -16:
				return "LONGNVARCHAR";
			case 2011:
				return "NCLOB";
			case 2009:
				return "SQLXML";
			case 2012:
				return "REF_CURSOR";
			case 2013:
				return "TIME_WITH_TIMEZONE";
			case 2014:
				return "TIMESTAMP_WITH_TIMEZONE";
			default:
				return "0000";
		}
	}

	@Override
	public String exportSql(TableBean tableBean) {

		StringBuffer sbTable = new StringBuffer();
		StringBuffer sbComments = new StringBuffer();

		if ("TABLE".equalsIgnoreCase(tableBean.getType())) {
			sbTable.append("CREATE TABLE public." + tableBean.getTableName() + " (");
		} else if ("VIEW".equalsIgnoreCase(tableBean.getType())) {
			sbTable.append("CREATE VIEW `" + tableBean.getTableName() + "` (");
		}
		//动态拼接字段
		for (ColumnBean column : tableBean.getColumnList()) {
			String columnType = java2db(column.getColumnTypeName().toUpperCase());
			sbTable.append(column.getColumnName() + " " + columnType + " ");

			if (column.getIsPk()) {
				sbTable.append("NOT NULL PRIMARY KEY ");
			} else {
				if (columnType.contains("CHAR")) {
					sbTable.append("(" + column.getColumnSize() + ") ");
				} else if (columnType.contains("NUMERIC")) {
					sbTable.append("(" + column.getColumnSize() + "," + column.getDecimalDigits() + ") ");
				} else {
					sbTable.append(" ");
				}
				//sbTable.append(column.getIsNullable() ? "NULL " : "NOT NULL ");
			}
			if (!column.getIsPk() && !"".equals(column.getDefaultValue())) {
				sbTable.append(" DEFAULT '" + column.getDefaultValue().trim() + "',");
			} else {
				sbTable.append(",");
			}
			sbComments.append("COMMENT ON COLUMN   " + tableBean.getTableName() + "." + column.getColumnName() + " IS  '" + column.getRemark() + "';");
		}
		sbTable = new StringBuffer(sbTable.toString().substring(0, sbTable.toString().length() - 1));
		sbTable.append("); ");

		return sbTable.append(sbComments).toString();
	}

	@Override
	public String java2db(String dataType) {
		switch (dataType) {
			case "TINYINT":
				return "SMALLINT";
			case "DOUBLE":
				return "NUMERIC";
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
