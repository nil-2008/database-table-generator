package com.inspur;

import com.inspur.rdms.DatabaseFactory;
import com.inspur.rdms.bean.TableBean;
import com.inspur.rdms.impl.DatabaseImpl;

/**
 * 程序主入口
 *
 * @author Neo
 * @date 2019/2/26 10:49
 */
public class Index {
	public static void main(String[] args) {
		DatabaseImpl to = DatabaseFactory.getDatabase("postgresql", "test_db", "127.0.0.1", 5432, "inspur", "root", "postgresql");
		DatabaseImpl from = DatabaseFactory.getDatabase("mysql", "test_db", "10.110.16.96", 3306, "root", "123456a?", "mysql");
		TableBean tableBean = null;
		if (from != null) {
			tableBean = from.getSourceTableColumns("", "t_user_444");
		} else {
			return;
		}

		if (to != null && tableBean != null) {
			Boolean info = to.createTableFromSource(tableBean);
			System.out.println(info);
		}
	}
}
