package com.secneo.hbase.utils;

import java.util.List;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.lang.builder.ToStringBuilder;

public class Config {
	
	public static final Config i = new Config();

	@SuppressWarnings("unchecked")
	private Config() {
		PropertiesConfiguration c = null;
		try {
			c = new PropertiesConfiguration("config.conf");
		} catch (ConfigurationException e) {
			e.printStackTrace();
			System.exit(1);
		}
		
		this.HOST = c.getString("HOST", "name1.bang001.com");
		this.PORT = c.getInt("PORT", 9090);
		this.SCAN_SIZE = c.getInt("SCAN_SIZE", 1000);
		this.TABLE = c.getString("TABLE");
		this.FAMILY = c.getString("FAMILY");
		this.COLUMN = c.getList("COLUMN");
		this.TABLE_AN = c.getString("TABLE_AN");
		this.FAMILY_AN = c.getString("FAMILY_AN");
		this.COLUMN_AN = c.getString("COLUMN_AN");
		
		//udid的样例
		this.UDID_em = c.getString("UDID_em", "01ac3975-76b5-36c4-b468-a9a6a08fb2d1");
		this.APK_LIST = c.getList("APK_LIST");
		this.TOOL_LIST = c.getList("TOOL_LIST");
        
		this.driver = c.getString("driver");
		this.url = c.getString("url");
		this.user = c.getString("user");
		this.password = c.getString("password");
	}
	
	/** HBase */
	public final String HOST;
	public final int PORT;
	public final int SCAN_SIZE;
    public final String TABLE;
    public final String FAMILY;
    public final List<String> COLUMN;
    public final String TABLE_AN;
    public final String FAMILY_AN;
    public final String COLUMN_AN;
    
	/** MySql */
	public final String driver;
	public final String url;
	public final String user;
	public final String password;
	
	public final String UDID_em;
	public final List<String> APK_LIST;
	public final List<String> TOOL_LIST;
	                   
	@Override
	public String toString() {
		return ToStringBuilder.reflectionToString(this);
	}
}