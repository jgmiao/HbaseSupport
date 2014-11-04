package com.secneo.hbase.apkname;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.hbase.thrift2.generated.TColumnValue;
import org.apache.hadoop.hbase.thrift2.generated.TIOError;
import org.apache.hadoop.hbase.thrift2.generated.TResult;
import org.apache.thrift.TException;

import com.google.gson.Gson;
import com.secneo.hbase.HbaseScanner;
import com.secneo.hbase.utils.Config;
import com.secneo.hbase.utils.Utils;
import com.secneo.hbase.utils.WriteFile;

/**
 * 修饰父类的HbaseCore中scan方法 装饰成定制的Scanner完成ApkName的扫描
 */
public class ApknameScanner extends HbaseScanner {

	private static final int BUFFER = 512;
	
	public ApknameScanner(String host, int port) {
		super(host, port);
	}
	
	/**
	 * 修饰父类的scan方法
	 * @param prefix
	 * @return apkName结果集
	 * @throws TIOError
	 * @throws TException
	 */
	public HashMap<String, Integer> buildNameMap() throws TIOError, TException {
		byte[] start = null;
		ByteBuffer table = Utils.wrap(Config.i.TABLE_AN);
		List<TResult> tResultList = null;
		HashMap<String, Integer> apkNameMap = new HashMap<String, Integer>(BUFFER);
		
		/**
		 * 调用父类的scan方法
		 */
		while (null != (tResultList = scan(start, null, table, Config.i.FAMILY_AN, Config.i.COLUMN_AN))) {
			int tmpResSize = tResultList.size();
			if (tmpResSize <= 0) {
				break;
			}
			
			for (TResult tResult : tResultList) {
				List<TColumnValue> columns = tResult.getColumnValues();
				for (TColumnValue c : columns) {
					String tmpStr = Utils.convert(c.getValue());
					if (apkNameMap.containsKey(tmpStr)) {
						apkNameMap.put(tmpStr, apkNameMap.get(tmpStr) + 1);
					} else {
						apkNameMap.put(tmpStr, 1);
					}
				}
			}
			
			if (tmpResSize < Config.i.SCAN_SIZE) { //如果没有取满了
				break;
			}
			start = tResultList.get(tmpResSize - 1).getRow();
//			System.out.println(new String(start));
		}//while
		return apkNameMap;
	}//buildNameMap
	
	public static void main(String[] args) {
		ApknameScanner scanner = new ApknameScanner(Config.i.HOST, Config.i.PORT);
		Gson gson = new Gson();
		try {
			final long s = System.nanoTime();
			HashMap<String, Integer> nameMap = scanner.buildNameMap();
			WriteFile.write(gson.toJson(nameMap), Config.i.ApknameStorageFile);
			System.out.println("DisposeScanner完成, 用时： " + (System.nanoTime() - s)/1.0e9 + "s");
		} catch (TIOError e) {
			e.printStackTrace();
		} catch (TException e) {
			e.printStackTrace();
		}
	}
}