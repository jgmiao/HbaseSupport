package com.secneo.hbase.udid;

import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.apache.hadoop.hbase.thrift2.generated.TResult;

import com.secneo.hbase.jdbc.JDBC;
import com.secneo.hbase.utils.Config;
import com.secneo.hbase.utils.Utils;

/**
 * 找出给定应用名同时安装各种破解器的设备量情况
 * 如找出“英雄三国”和其他各种破解器同时存在与一个设备的UDID
 */
public class DisposeScanner {
	
	private static final int BUFFER = 512;
	private static final String UDID_EP = Config.i.UDID_em;
	private static final int UDID_EP_LEN = UDID_EP.length();
	private static UdidScanner SCANNER = null;
	
	static {
		SCANNER = new UdidScanner(Config.i.HOST, Config.i.PORT);
	}

	/**
	 * TODO 先统计出各自包名
	 * 判断优先找出那一组注入内存  后面遍历依次比对
	 */
	private void dispose(List<String> apkNames, List<String> toolNames) {
		if (!(null != apkNames && apkNames.size() > 0 && null != toolNames && toolNames.size() > 0)) {
			Utils.log.error("dispose传入参数有误，请检查！");
			return;
		}
		
		List<String> loopList = null;	
		HashMap<String, HashSet<String>> loopMap = null; 
		HashMap<String, HashSet<String>> apkUDIDMap = null; //指定引用的映射集
		HashMap<String, HashSet<String>> toolUDIDMap = null;//破解工具的映射集

		Connection conn = JDBC.getConnection();
		
		if (apkNames.size() < toolNames.size()) {
			apkUDIDMap = new HashMap<String, HashSet<String>>(BUFFER); 
			/** 对指定应用 */
			for (String apkName : apkNames) {
				HashSet<String> uSet = disposeSQL(conn, apkName);
				if (null != uSet) {
					apkUDIDMap.put(apkName, uSet);
				} else {
					Utils.log.warn(String.format("获取指定应用[%s]对应的UDID集合为空! ", apkName));
				}
			}
			//设置下面循环对象
			loopList = toolNames;
			loopMap = apkUDIDMap;
		} else {
			toolUDIDMap = new HashMap<String, HashSet<String>>(BUFFER);
			/** 对破解工具 */
			for (String toolName : toolNames) {
				HashSet<String> uSet = disposeSQL(conn, toolName);
				if (null != uSet) {
					toolUDIDMap.put(toolName, uSet);
				} else {
					Utils.log.warn(String.format("获取破解工具[%s]对应的UDID集合为空! ", toolName));
				}
			}
			//设置下面循环对象
			loopList = apkNames;
			loopMap = toolUDIDMap;
		}
		
		for (String name : loopList) {//for memory
			HashSet<String> uSet = disposeSQL(conn, name);
			if (null != uSet) { //如果uSet不为空， 则遍历loopMap 依次对比
				Iterator<Entry<String, HashSet<String>>> itor = loopMap.entrySet().iterator();
				while (itor.hasNext()) {
					Entry<String, HashSet<String>> entry = itor.next();
					
					int UNum = disposeUDID(uSet, entry.getValue());
					Utils.log.info(String.format("[%s, %s][%d, %d]同时安装的设备量为%d", name, entry.getKey(),
							uSet.size(), entry.getValue().size(), UNum));
				}
			} else {
				Utils.log.warn(String.format("获取[%s]对应的UDID集合为空! ", name));
			}
		}//for
		
		JDBC.closeConnection(conn);
		Utils.log.info("Scan finish!");
	}//dispose
	
	/**
	 * apkUDID的量较少，故 这里遍历apkUDID在toolUDID中是否存在
	 * <可用BitMap数据结构优化>
	 * 
	 * @return  重复UDID的个数
	 */
	private int disposeUDID(HashSet<String> set1, HashSet<String> set2) {
		if (!(null != set1 && set1.size() > 0 && null != set2 && set2.size() > 0)) {
			Utils.log.error("[disposeUDID]传入参数有误，返回0");
			return 0;
		}
		int uNum = 0;
		for (String apk : set1) {
			if (set2.contains(apk)) {
				uNum ++;
			}
		}
		return uNum;
	}
	
	/**
	 * @param conn 
	 * @param apkName
	 * @return HashSet<byte[]> 所有PKG对应的UDID集合
	 */
	private HashSet<String> disposeSQL(Connection conn, String apkName) {
		String sql = String.format("SELECT DISTINCT(a.pkg_name) FROM installed_apks a WHERE a.apk_name = '%s'", apkName);
		Utils.log.info("sql: " + sql);
		
		List<String> pkgList = null;
		try {
			pkgList = Utils.selectBySQL(conn, sql);
		} catch (SQLException e) {
			Utils.log.error("访问数据表installed_apks有误！  " + e.getMessage());
		}
		//构建apkName对应的UDID映射
		if (null != pkgList && pkgList.size() > 0) {
			return buildSet(pkgList);
		} else {
			Utils.log.error("Utils.selectBySQL = null, can not build mapping.");
			return null;
		}
	}//disposeSQL
	
	/**
	 * 整合该应用对于pkg查找出的UDID到一个集合Set
	 */
	private HashSet<String> buildSet(List<String> pkgList) {
		HashSet<String> resSet = new HashSet<String>();
		
		for(String pkgName : pkgList) {
			System.out.println("pkgName: " + pkgName);
			resSet.addAll(scan(pkgName, UDID_EP_LEN));
		}
		
		return resSet;
	}//buildSet
	
	/**
	 * 按pkgName为前缀查找对于的UDID
	 */
	private HashSet<String> scan(String pkgName, int udidLen) {
		HashSet<String> resSet = new HashSet<String>(BUFFER);
		List<TResult> tResultList = null;
		try {
			tResultList = SCANNER.udidScan(pkgName);
		} catch (Exception e) {
			Utils.log.error("HBase查找出错。" + e.getMessage());
			e.printStackTrace();
		}
		/** 从行键中提取UDID */
		for (TResult tResult : tResultList) {
			byte[] resRow = tResult.getRow(); // 行键
			byte[] resUDID = new byte[udidLen]; // UDID
			int step = resRow.length - udidLen; // 去掉前面非UDID部分
			for (int i = 0; i < udidLen; i++) {
				resUDID[i] = resRow[i + step];
			}
			resSet.add(Utils.convert(resUDID));
		}
		return resSet;
	}
	
	public static void main(String[] args) {
		DisposeScanner center = new DisposeScanner();
		
		List<String> apkNames = decode(Config.i.APK_LIST);
		List<String> toolNames = decode(Config.i.TOOL_LIST);
		
		center.dispose(apkNames, toolNames);
	}
	
	private static List<String> decode(List<String> enList) {
		List<String> deList = new ArrayList<String>(enList.size());
		try {
			for (String s : enList) {
				deList.add(new String(s.getBytes("ISO-8859-1"), "UTF-8"));
			}
		} catch (UnsupportedEncodingException e) {
			System.out.println("decode解码出错。 " + e.getMessage());
		}
		return deList;
	}
}