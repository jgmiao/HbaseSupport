package com.secneo.hbase.pkg_udid;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.thrift2.generated.TIOError;
import org.apache.hadoop.hbase.thrift2.generated.TResult;
import org.apache.thrift.TException;

import com.secneo.hbase.HbaseCore;
import com.secneo.hbase.utils.Config;
import com.secneo.hbase.utils.Utils;

public class UdidScanner extends HbaseCore {

	public UdidScanner(String host, int port) {
		super(host, port);
	}
	
	/**
	 * 前缀扫描 修饰父类的scan方法
	 * @param prefix
	 * @return
	 * @throws TIOError
	 * @throws TException
	 */
	public List<TResult> udidScan(String prefix) throws TIOError, TException {	
		byte[] start = null;
		byte[] stop = null;
		ByteBuffer table = Utils.wrap(Config.i.TABLE);
		List<TResult> tResult = new ArrayList<TResult>(Config.i.SCAN_SIZE);
		
		if (null != prefix && prefix.length() > 0) {
			start = Utils.wrap(prefix + "_00000000").array();
			stop =  Utils.wrap(prefix + "_ffffffff").array();
		}
		
		List<TResult> tmpRes = null;
		/**
		 * 调用父类的scan方法
		 */
		while (null != (tmpRes = scan(start, stop, table, Config.i.FAMILY, Config.i.COLUMN))) {
			int tmpResSize = tmpRes.size();
			if (tmpResSize <= 0) {
				break;
			}
			start = tmpRes.get(tmpResSize - 1).getRow();
			tResult.addAll(tmpRes);
			if (tmpResSize < Config.i.SCAN_SIZE) { // 如果没有取满了
				break;
			}
		}
		if (null == tmpRes && tResult.size() <= 0) {
			Utils.log.error("[null == tmpRes && tResult.size() <= 0], please check UdidScanner udidScan!");
		}
		Utils.log.info("tResult.size = " + tResult.size());
		return tResult;
	}//udidScan
}