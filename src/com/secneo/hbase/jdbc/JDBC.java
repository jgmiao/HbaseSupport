package com.secneo.hbase.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;

import com.secneo.hbase.utils.Config;

public class JDBC {
	
	static { //获取驱动是重量级操作，放在静态块中执行
		try {
			Class.forName(Config.i.driver);
		} catch (ClassNotFoundException e) {
			throw new RuntimeException("加载驱动类失败" + e.getMessage(), e);
		}
	}
	
	/**
	 * 获取连接
	 */
	public static Connection getConnection() {
		Connection conn = null;
		try {
			conn = DriverManager.getConnection(Config.i.url, Config.i.user, Config.i.password);
		} catch (Exception e) {
			throw new RuntimeException("数据库连接失败" + e.getMessage(), e);
		}
		return conn;
	}
	
	/**
	 * 关闭连接
	 * @param conn
	 */
	public static void closeConnection(Connection conn) {
		try {
			if (null != conn && !conn.isClosed()) {
				conn.close();
			}
		} catch (Exception e) {
			throw new RuntimeException(e.getMessage(), e);
		}
	}
}