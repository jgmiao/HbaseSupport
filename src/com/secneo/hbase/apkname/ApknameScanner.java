package com.secneo.hbase.apkname;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.thrift2.generated.TColumnValue;
import org.apache.hadoop.hbase.thrift2.generated.TIOError;
import org.apache.hadoop.hbase.thrift2.generated.TResult;
import org.apache.thrift.TException;

import com.secneo.hbase.HbaseCore;
import com.secneo.hbase.utils.Config;
import com.secneo.hbase.utils.Utils;

public class ApknameScanner extends HbaseCore {

	public ApknameScanner(String host, int port) {
		super(host, port);
	}
	
	/**
	 * 修饰父类的scan方法
	 * @param prefix
	 * @return
	 * @throws TIOError
	 * @throws TException
	 */
	public List<TResult> nameScan() throws TIOError, TException {
		int iNum = 0;
		
		byte[] start = null;
		ByteBuffer table = Utils.wrap(Config.i.TABLE_AN);
		List<TResult> tmpRes = null;
		List<TResult> tResult = new ArrayList<TResult>(Config.i.SCAN_SIZE);

		/**
		 * 调用父类的scan方法
		 */
		while (null != (tmpRes = scan(start, null, table, Config.i.FAMILY_AN, Config.i.COLUMN_AN)) && iNum++ < 5) {
			int tmpResSize = tmpRes.size();
			if (tmpResSize <= 0) {
				break;
			}
			start = tmpRes.get(tmpResSize - 1).getRow();
			tResult.addAll(tmpRes);
			if (tmpResSize < Config.i.SCAN_SIZE) { //如果没有取满了
				break;
			}
		}
		if (null == tmpRes && tResult.size() <= 0) {
			Utils.log.error("[null == tmpRes && tResult.size() <= 0], please check ApknameScanner nameScan!");
		}
		Utils.log.info("tResult.size = " + tResult.size());
		return tResult;
	}//nameScan
	
	public static void main(String[] args) {
		ApknameScanner scanner = new ApknameScanner(Config.i.HOST, Config.i.PORT);
		try {
			List<TResult> tResultList = scanner.nameScan();
			for (TResult tResult : tResultList) {
//				System.out.println(Utils.convert(tResult.getRow()));
				List<TColumnValue> cList = tResult.getColumnValues();
				for (TColumnValue c : cList) {
					System.out.println(Utils.convert(c.getValue()));
				}
			}
		} catch (TIOError e) {
			e.printStackTrace();
		} catch (TException e) {
			e.printStackTrace();
		}
	}
}