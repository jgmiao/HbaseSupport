package com.secneo.hbase.utils;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;
import org.apache.log4j.Logger;


public class Utils {
	
	public static Logger log = Logger.getLogger(Utils.class);
	
	public static List<String> selectBySQL(Connection conn, String sql) throws SQLException {
		QueryRunner query = new QueryRunner();
		List<String> objs = query.query(conn, sql, new ResultSetHandler<List<String>>(){
			@Override
			public List<String> handle(ResultSet res) throws SQLException {
				List<String> list = new ArrayList<String>();
				while (res.next()) {
					list.add(res.getString("pkg_name"));
				}
				return list;
			}
		});
		return objs;
	}
	
	/**
	 * 将字符串转换成ByteBuffer
	 */
	public static ByteBuffer wrap(String str) {
		return ByteBuffer.wrap(str.getBytes());
	}

	/**
	 * 转化 byte[] -> String (UTF-8)
	 */
	public static String convert(byte[] bytes) {
		try {
			return (new String(bytes, "UTF-8"));
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		return null;
	}

	public static String convert(ByteBuffer buffer) {
		Charset charset = null;
		CharsetDecoder decoder = null;
		CharBuffer charBuffer = null;
		try {
			charset = Charset.forName("UTF-8");
			decoder = charset.newDecoder();
			charBuffer = decoder.decode(buffer);
			return charBuffer.toString();
		} catch (Exception ex) {
			ex.printStackTrace();
			return "";
		}
	}
}