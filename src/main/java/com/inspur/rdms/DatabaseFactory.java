package com.inspur.rdms;

import com.inspur.rdms.adapter.AbstractDatabase;
import com.inspur.rdms.adapter.MysqlAdapter;
import com.inspur.rdms.adapter.PostgresqlAdapter;
import com.inspur.rdms.impl.DatabaseImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 获取数据库处理类的工厂类
 *
 * @author Neo
 * @date 2019/2/26 11:05
 */
public class DatabaseFactory {
	private static Logger log = LoggerFactory.getLogger(DatabaseFactory.class);

	/**
	 * 获取数据库处理handler类的工厂类方法
	 *
	 * @param dbType       数据库类型
	 * @param databaseName 数据库名称
	 * @param host         ip地址
	 * @param port         端口号
	 * @param username     用户名
	 * @param password     密码
	 * @param jdbcType     oracle用到,serviceName和sid
	 * @return IDatabase 数据库处理handler
	 */
	@SuppressWarnings("unchecked")
	public static DatabaseImpl getDatabase(String dbType, String databaseName, String host, int port, String username, String password, String jdbcType) {

		if (null == dbType || "".equals(dbType)) {
			log.error("dbType为空");
			return null;
		}
		AbstractDatabase databaseHandler = null;
		if ("mysql".equalsIgnoreCase(dbType) && null == databaseHandler) {
			databaseHandler = new MysqlAdapter(databaseName, host, port, username, password, jdbcType);
		}

		if ("postgresql".equalsIgnoreCase(dbType) && null == databaseHandler) {
			databaseHandler = new PostgresqlAdapter(databaseName, host, port, username, password, jdbcType);
		}
		if (!databaseHandler.canConnect()) {
			log.error("服务器" + host + "无法连接");
			return null;
		}
		if (!databaseHandler.isDatabaseConnect()) {
			log.error("服务器上" + host + "上的数据库" + databaseName + "无法连接");
			return null;
		}
		return databaseHandler;
	}
}
