package com.inspur.rdms.adapter;

import com.alibaba.druid.pool.DruidDataSource;
import com.inspur.rdms.bean.ColumnBean;
import com.inspur.rdms.bean.TableBean;
import com.inspur.rdms.exception.DatabaseException;
import com.inspur.rdms.exception.ExceptionEnum;
import com.inspur.rdms.impl.DatabaseImpl;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.net.telnet.TelnetClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * DataBase抽象方法 通用方法+需要各个handler实现的接口
 *
 * @author Neo
 * @date 2019/2/22 8:06
 */
@Setter
@Getter
public abstract class AbstractDatabase implements DatabaseImpl {
	private static Logger log = LoggerFactory.getLogger(AbstractDatabase.class);

	/**
	 * Druid连接池对象 -> abstract处理
	 */
	private DruidDataSource druidDataSource;
	/**
	 * JDBCUrl-》子类传入handler方法
	 */
	private String jdbcUrl;

	/**
	 * 数据库相关配置-》外部传入构造函数
	 * 数据库名
	 */
	private String databaseName;
	/**
	 * IP地址
	 */
	private String host;
	/**
	 * 端口
	 */
	private int port;
	/**
	 * 用户名
	 */
	private String username;
	/**
	 * 密码
	 */
	private String password;
	/**
	 * oracle专用
	 */
	private String jdbcType;

	public AbstractDatabase(
		String databaseName,
		String host,
		int port,
		String username,
		String password,
		String jdbcType) {
		super();
		this.databaseName = databaseName;
		this.host = host;
		this.port = port;
		this.username = username;
		this.password = password;
		this.jdbcType = jdbcType;
		this.setJdbcUrl(this.handleJdbcUrl(jdbcType));
	}

	/**
	 * 判断服务器是否可以连接
	 *
	 * @return boolean true:可用 false:不可用
	 */
	@Override
	public boolean canConnect() {
		TelnetClient telnet = new TelnetClient();
		try {
			telnet.connect(this.host, this.port);
			return telnet.isConnected();
		} catch (IOException e) {
			log.info("ip地址和端口号不通");
			return false;
		}
	}

	/**
	 * 数据库是否可以连接
	 *
	 * @return boolean  true:可用 false:不可用
	 */
	@Override
	public boolean isDatabaseConnect() {
		boolean isDatabaseConnect = false;
		String driver = this.handleDriver();
		Connection conn = null;

		try {
			Class.forName(driver);
			//设置连接时间
			DriverManager.setLoginTimeout(5);
			conn = DriverManager.getConnection(this.jdbcUrl, this.username, this.password);
			isDatabaseConnect = true;
		} catch (ClassNotFoundException | SQLException e) {
			//返回是否可以连接，异常信息写入日志
			log.info("数据库连接失败", e);
		} finally {
			this.closeObject(conn, null, null);
		}
		return isDatabaseConnect;
	}

	/**
	 * 执行SQL语句
	 *
	 * @param sql 要执行的SQL
	 */
	@Override
	public void executeSql(String sql) {
		log.info("即将执行SQL语句:" + sql);
		Connection conn = null;
		PreparedStatement psmt = null;

		try {
			conn = this.getConnection();
			psmt = conn.prepareStatement(sql);
			//executeUpdate 的返回值是一个整数（int），指示受影响的行数（即更新计数）。
			//对于 CREATE TABLE 或 DROP TABLE 等不操作行的语句，executeUpdate 的返回值总为零。
			psmt.executeUpdate();
		} catch (SQLException e) {
			throw new DatabaseException(ExceptionEnum.SQL_EXECUTE_EXCEPTION, "executeSql", "执行sql:" + sql + "语句时异常" + e);
		} finally {
			this.closeObject(conn, psmt, null);
		}
	}

	/**
	 * 返回数据库连接
	 *
	 * @return Connection 数据库连接
	 */
	@Override
	public Connection getConnection() {
		DruidDataSource ds;
		Connection conn;
		//druid单例模式
		if (null == this.druidDataSource) {
			log.info("GetConnection: create druid pool object");
			String driver = this.handleDriver();

			ds = new DruidDataSource();
			//这一项可配可不配，如果不配置druid会根据url自动识别dbType，然后选择相应的driverClassName(建议配置下)
			ds.setDriverClassName(driver);
			ds.setUrl(this.jdbcUrl);
			//连接数据库的用户名
			ds.setUsername(this.username);
			//连接数据库的密码。如果你不希望密码直接写在配置文件中，可以使用ConfigFilter。
			ds.setPassword(this.password);
			//初始化时建立物理连接的个数。初始化发生在显示调用init方法，或者第一次getConnection时.默认值0
			ds.setInitialSize(0);
			//最大连接池数量,默认值8
			ds.setMaxActive(2);
			//获取连接时最大等待时间，单位毫秒。配置了maxWait之后，缺省启用公平锁，并发效率会有所下降，如果需要可以通过配置useUnfairLock属性为true使用非公平锁。
			ds.setMaxWait(5000);
			// 配置间隔多久启动一次DestroyThread，对连接池内的连接才进行一次检测，单位是毫秒
			ds.setTimeBetweenEvictionRunsMillis(10);
			// 连接长时间不归还，强制归还
			ds.setRemoveAbandoned(true);
			ds.setRemoveAbandonedTimeout(80);
			// 关闭自动重试
			ds.setConnectionErrorRetryAttempts(0);
			ds.setBreakAfterAcquireFailure(true);

			this.setDruidDataSource(ds);

			log.info("create druid pool success!");
			log.info("driver:{}|jdbcUrl:{}|this.username:{}", driver, this.jdbcUrl, this.username);
		}

		try {
			conn = this.druidDataSource.getConnection();
		} catch (SQLException e) {
			throw new DatabaseException(ExceptionEnum.DATASOURCE_CONNECT_EXCEPTION, "getConnection", "获取数据源连接失败");
		}
		return conn;
	}

	/**
	 * 获取JDBCUrl :整合不同类型的数据库
	 *
	 * @return String 不同类型的数据库连接
	 */
	@Override
	public String getJDBCUrl() {
		//jdbcType:目前该参数是Oracle用到的
		return this.handleJdbcUrl(this.jdbcType);
	}

	@Override
	public List<Map<String, Object>> querySchema() {
		List<Map<String, Object>> result = new ArrayList<>();

		Connection conn = this.getConnection();
		ResultSet rs = null;

		try {
			DatabaseMetaData dbma = conn.getMetaData();
			rs = dbma.getSchemas();

			while (rs.next()) {
				Map<String, Object> resultMap = new HashMap<>();
				resultMap.put("TABLE_SCHEM", rs.getObject("TABLE_SCHEM"));
				result.add(resultMap);
			}

		} catch (SQLException e) {
			throw new DatabaseException(ExceptionEnum.DATABASE_STRUCTURE_EXCEPTION, "querySchema", "querySchema出现异常");
		} finally {
			this.closeObject(conn, null, rs);
		}
		return result;
	}

	/**
	 * 获取模式
	 *
	 * @param schema 模式
	 * @return String 返回数据库的模式
	 */
	@Override
	public String getSchema(String schema) {
		return this.handleSchema(schema);
	}

	/**
	 * 表是否存在
	 *
	 * @param tableName 表名
	 * @return boolean  true:存在 false:不存在
	 */
	@Override
	public boolean isTableExists(String tableName) {
		boolean isExist = false;
		Connection conn = null;
		ResultSet rs = null;

		try {
			conn = this.getConnection();
			DatabaseMetaData dbmd = conn.getMetaData();
			rs = dbmd.getTables(conn.getCatalog(), null, tableName, new String[]{"TABLE"});
			while (rs.next()) {
				String tableSchem = rs.getString("TABLE_SCHEM");
				// postgresql的TABLE_CAT为null
				if (null == rs.getString("TABLE_CAT")) {
					if (tableSchem.equals(this.getSchema(""))) {
						return true;
					}
				}
				if (StringUtils.isNotBlank(tableSchem)) {
					if (tableSchem.equals(this.getSchema(""))) {
						if (this.databaseName.equals(rs.getString("TABLE_CAT"))) {
							isExist = true;
						}
					}
				} else {
					if (this.databaseName.equals(rs.getString("TABLE_CAT"))) {
						isExist = true;
					}
				}
			}
		} catch (SQLException e) {
			log.info("数据表" + tableName + "不存在");
			return false;
			//throw new DatabaseException(ExceptionEnum.DATABASE_STRUCTURE_EXCEPTION, "isTableExist", "isTableExist出现异常");
		} finally {
			this.closeObject(conn, null, rs);
		}
		return isExist;
	}

	/**
	 * 查询表
	 *
	 * @param schema 模式
	 * @param type   类别
	 * @return List 表字段信息
	 */
	@Override
	public List<TableBean> queryTable(String schema, String type) {
		List<TableBean> result = new ArrayList<>();
		Connection conn = null;
		ResultSet rs = null;

		try {
			conn = this.getConnection();
			DatabaseMetaData dbmd = conn.getMetaData();
			String[] types;
			if (StringUtils.isNoneEmpty(type)) {
				types = new String[1];
				types[0] = type;
			} else {
				String[] tempType = {"TABLE", "VIEW"};
				types = tempType;
			}
			rs = dbmd.getTables(conn.getCatalog(), schema, null, types);
			while (rs.next()) {
				TableBean tableBean = new TableBean();
				Map<String, Object> resultMap = new HashMap<>();
				//表模式（可能为空），在Oracle中获取的是命名空间
				resultMap.put("TABLE_SCHEM", rs.getString("TABLE_SCHEM"));
				//表名
				tableBean.setTableName(rs.getString("TABLE_NAME"));
				//表类型，典型的类型有：
				// "TABLE"、"VIEW"、"SYSTEM TABLE"、"GLOBAL TEMPORARY"、"LOCAL TEMPORARY"、"ALIAS" 和"SYNONYM"
				tableBean.setType(rs.getString("TABLE_TYPE"));
				//表备注
				tableBean.setRemark(rs.getString("REMARKS"));
				result.add(tableBean);
			}
		} catch (SQLException e) {
			throw new DatabaseException(
				ExceptionEnum.DATABASE_STRUCTURE_EXCEPTION, "queryTable", "queryTable出现异常");
		} finally {
			this.closeObject(conn, null, rs);
		}
		return result;
	}

	/**
	 * 返回数据表行的信息
	 *
	 * @param schema    数据库的schema
	 * @param tableName 表名
	 * @return TableBean  表行的信息
	 */
	@Override
	public ArrayList<ColumnBean> queryTableColumns(String schema, String tableName) {
		ArrayList<ColumnBean> columnBeanList = new ArrayList<>();
		List<Map<String, Object>> pkList = queryTablePrimaryKey(schema, tableName);
		Connection conn = null;
		ResultSet rs = null;

		try {
			conn = this.getConnection();
			//处理连接属性，目前oracle用到
			this.handleRemarks(conn);
			DatabaseMetaData dbmd = conn.getMetaData();
			rs = dbmd.getColumns(conn.getCatalog(), schema, tableName, null);

			while (rs.next()) {
				ColumnBean columnBean = new ColumnBean();

				// 是否主键 0：不是 1：是
				boolean isPrimaryKey = false;
				for (Map<String, Object> pkMap : pkList) {
					if (pkMap.get("COLUMN_NAME").toString().equals(rs.getString("COLUMN_NAME"))) {
						isPrimaryKey = true;
					}
				}
				columnBean.setIsPk(isPrimaryKey);

				//列名
				columnBean.setColumnName(rs.getString("COLUMN_NAME"));
				//对应的java.sql.Types类型及名字
				columnBean.setColumnType(rs.getInt("DATA_TYPE"));
				columnBean.setColumnTypeName(getTypeById(rs.getInt("DATA_TYPE")));
				//列宽度和精度
				columnBean.setColumnSize(rs.getInt("COLUMN_SIZE"));
				columnBean.setDecimalDigits(rs.getInt("DECIMAL_DIGITS"));
				//是否允许使用NULL（0：否 1：是）
				columnBean.setIsNullable("0".equals(rs.getString("NULLABLE")));
				//描述列的注释（可能为null）
				columnBean.setRemark((rs.getString("REMARKS") == null) ? "" : rs.getString("REMARKS"));
				//默认值
				if (rs.getString("COLUMN_DEF") == null) {
					columnBean.setDefaultValue("");
				} else {
					String defaultValue = rs.getString("COLUMN_DEF");
					if (defaultValue.contains("::")) {
						int end = defaultValue.lastIndexOf("::");
						defaultValue = defaultValue.substring(1, end - 1);
					}
					columnBean.setDefaultValue(defaultValue);
				}
				columnBeanList.add(columnBean);
			}

		} catch (SQLException e) {
			throw new DatabaseException(
				ExceptionEnum.DATABASE_STRUCTURE_EXCEPTION, "queryTableColumns", "getColumnsList出现异常");
		} finally {
			this.closeObject(conn, null, rs);
		}
		return columnBeanList;
	}

	/**
	 * 查询表的索引
	 *
	 * @param schema    模式
	 * @param tableName 表名
	 * @return List 表索引信息
	 */
	@Override
	public List queryTableIndex(String schema, String tableName) {
		List<Map<String, Object>> result = new ArrayList<>();
		Connection conn = null;
		ResultSet rs = null;

		try {
			conn = this.getConnection();
			DatabaseMetaData dbmd = conn.getMetaData();
			/**
			 * unique - 该参数为 true 时，仅返回唯一值的索引；该参数为 false 时，返回所有索引，不管它们是否唯一
			 * approximate - 该参数为 true时，允许结果是接近的数据值或这些数据值以外的值；该参数为 false 时，要求结果是精确结果
			 */
			boolean unique = false;
			boolean approximate = true;
			rs = dbmd.getIndexInfo(conn.getCatalog(), schema, tableName, unique, approximate);
			while (rs.next()) {
				Map<String, Object> resultMap = new HashMap<>();
				//表模式（可以为NUll）
				resultMap.put("TABLE_SCHEM", rs.getObject("TABLE_SCHEM"));
				//表名称
				resultMap.put("TABLE_NAME", rs.getString("TABLE_NAME"));
				//索引值是否可以唯一
				resultMap.put("NON_UNIQUE", rs.getString("NON_UNIQUE"));
			}
		} catch (SQLException e) {
			throw new DatabaseException(
				ExceptionEnum.DATABASE_STRUCTURE_EXCEPTION, "queryTableIndex", "queryTableIndex出现异常");
		} finally {
			this.closeObject(conn, null, rs);
		}
		return result;
	}

	/**
	 * 获取数据表的主键
	 *
	 * @param schema    模式
	 * @param tableName 表名
	 * @return List 主键信息
	 */
	@Override
	public List queryTablePrimaryKey(String schema, String tableName) {
		List<Map<String, Object>> result = new ArrayList<>();
		Connection conn = null;
		ResultSet rs = null;

		try {
			conn = this.getConnection();
			DatabaseMetaData dbmd = conn.getMetaData();
			rs = dbmd.getPrimaryKeys(conn.getCatalog(), schema, tableName);

			while (rs.next()) {
				Map<String, Object> resultMap = new HashMap<>();
				//表模式（可以为NUll）
				resultMap.put("TABLE_SCHEM", rs.getObject("TABLE_SCHEM"));
				//表名称
				resultMap.put("TABLE_NAME", rs.getString("TABLE_NAME"));
				//列名
				resultMap.put("COLUMN_NAME", rs.getString("COLUMN_NAME"));
				// 主键中的序列号（值1表示主键中的第一列，值2表示主键中的第二列）。
				resultMap.put("KEY_SEQ", rs.getString("KEY_SEQ"));
				//主键的名称（可为null）
				resultMap.put("PK_NAME", rs.getString("PK_NAME"));

				result.add(resultMap);
			}

		} catch (SQLException e) {
			throw new DatabaseException(
				ExceptionEnum.DATABASE_STRUCTURE_EXCEPTION,
				"queryTablePrimaryKey",
				"queryTablePrimaryKey出现异常");
		} finally {
			this.closeObject(conn, null, rs);
		}
		return result;
	}

	/**
	 * 关闭资源
	 *
	 * @param conn Connection
	 * @param psmt PreparedStatement
	 * @param rs   ResultSet
	 */
	@Override
	public void closeObject(Connection conn, PreparedStatement psmt, ResultSet rs) {
		if (null != rs) {
			try {
				rs.close();
			} catch (SQLException e1) {
				throw new DatabaseException(
					ExceptionEnum.CLOSE_CONNECT_EXCEPTION, "closeObject", "关闭rs出现异常");
			}
		}

		if (null != psmt) {
			try {
				psmt.close();
			} catch (SQLException e2) {
				throw new DatabaseException(
					ExceptionEnum.CLOSE_CONNECT_EXCEPTION, "closeObject", "关闭psmt出现异常");
			}
		}
		if (null != conn) {
			try {
				conn.close();
			} catch (SQLException e3) {
				throw new DatabaseException(
					ExceptionEnum.CLOSE_CONNECT_EXCEPTION, "closeObject", "关闭conn出现异常");
			}
		}
	}

	/**
	 * 数据库连接 jdbc
	 *
	 * @return String  getDriverClass
	 */
	@Override
	public String getDriverClass() {
		return this.handleDriver();
	}

	/**
	 * java.sql.type2
	 *
	 * @param type
	 * @return
	 */
	public String getTypeById(int type) {
		switch (type) {
			case -7:
				return "BIT";
			case -6:
				return "TINYINT";
			case 5:
				return "SMALLINT";
			case 4:
				return "INTEGER";
			case -5:
				return "BIGINT";
			case 6:
				return "FLOAT";
			case 7:
				return "REAL";
			case 8:
				return "DOUBLE";
			case 2:
				return "NUMERIC";
			case 3:
				return "DECIMAL";
			case 1:
				return "CHAR";
			case 12:
				return "VARCHAR";
			case -1:
				return "LONGVARCHAR";
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
	public TableBean getSourceTableColumns(String schema, String tableName) {
		List<TableBean> tableBeans = this.queryTable(schema, "");
		TableBean table = new TableBean();
		table.setTableName(tableName);
		for (TableBean t : tableBeans) {
			if (tableName.equalsIgnoreCase(t.getTableName())) {
				table.setType(t.getType());
				table.setRemark(t.getRemark());
			}
		}
		//获取数据表字段描述
		List<ColumnBean> columnBeans = this.queryTableColumns(schema, tableName);
		table.setColumnList(columnBeans);
		return table;
	}
}
