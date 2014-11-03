package com.secneo.hbase.apkname;

import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.hbase.thrift2.generated.TColumnValue;
import org.apache.hadoop.hbase.thrift2.generated.TIOError;
import org.apache.hadoop.hbase.thrift2.generated.TResult;
import org.apache.thrift.TException;

import com.google.gson.Gson;
import com.secneo.hbase.utils.Config;
import com.secneo.hbase.utils.Utils;
import com.secneo.hbase.utils.WriteFile;

/**
 * 每天定时任务
 * 扫描出全表的apk_name,生成分词的数据源
 */
public class DisposeScanner {
	
	private static final int BUFFER = 512;
	private static final ApknameScanner SCANNER;
	
	static {
		SCANNER = new ApknameScanner(Config.i.HOST, Config.i.PORT);
	}
	
	public void dispose() {
		Gson gson = new Gson();
		final long s = System.nanoTime();
		HashMap<String, Integer> nameMap = buildMap();
		WriteFile.write(gson.toJson(nameMap), Config.i.ApknameStorageFile);
		System.out.println("DisposeScanner完成, 用时： " + (System.nanoTime() - s)/1.0e9 + "s");
	}
	
	private HashMap<String, Integer> buildMap() {
		List<TResult> tResultList;
		HashMap<String, Integer> apkNameMap = new HashMap<String, Integer>(BUFFER);
		try {
			tResultList = SCANNER.nameScan();
			for (TResult tResult : tResultList) {
				List<TColumnValue> cList = tResult.getColumnValues();
				for (TColumnValue c : cList) {
					String tmpStr = Utils.convert(c.getValue());
					if (apkNameMap.containsKey(tmpStr)) {
						apkNameMap.put(tmpStr, apkNameMap.get(tmpStr) + 1);
					} else {
						apkNameMap.put(tmpStr, 1);
					}
				}
			}
		} catch (TIOError e) {
			e.printStackTrace();
		} catch (TException e) {
			e.printStackTrace();
		}
		return apkNameMap;
	}//buildMap
	
	public static void main(String[] args) {
		DisposeScanner scanner = new DisposeScanner();
		scanner.dispose();
	}
}