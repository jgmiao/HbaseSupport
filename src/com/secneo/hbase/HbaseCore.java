package com.secneo.hbase;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.thrift2.generated.TColumn;
import org.apache.hadoop.hbase.thrift2.generated.THBaseService;
import org.apache.hadoop.hbase.thrift2.generated.TIOError;
import org.apache.hadoop.hbase.thrift2.generated.TResult;
import org.apache.hadoop.hbase.thrift2.generated.TScan;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import com.secneo.hbase.utils.Config;
import com.secneo.hbase.utils.Utils;

/**
 * 由此类获取封装的Scanner 需要传入参数Config.i.HOST, Config.i.PORT
 * 
 * 由Scanner返回List<TResult>
 * 
 * @author JGMiao
 */
public class HbaseCore {
	private static ClientSupport CLIENT = null;
	
	private String[] filters = null;
	
	public HbaseCore(String host, int port){
		if (null == host) {
			return;
		}
		if (null != CLIENT) {
			if (!(CLIENT.host == host.intern() && CLIENT.port == port)) {
				CLIENT = new ClientSupport(host, port);
			} 
		} else {
			CLIENT = new ClientSupport(host, port);
		}
	}
	
	public void SetFilter(String... filter) {
		this.filters = filter;
	}
	
	/**
	 * HBase Scan
	 * @param startRow 起始行键
	 * @param stopRow 结束行键
	 * @param table 待查询表名
	 * @param family 待查询列族名
	 * @param qualifiers 待查询列名集合
	 * 
	 * @return List<TResult>
	 * @throws TIOError
	 * @throws TException
	 */
	public List<TResult> scan(byte[] startRow, byte[] stopRow, ByteBuffer table, String family, List<String> qualifiers) throws TIOError, TException {
		List<TColumn> columns = new ArrayList<TColumn>();
		
		TColumn column = new TColumn();
		/** 列族的添加 */
		if (null != family && family.length() > 0) {
			column.setFamily(Utils.wrap(family));
		}
		/** 多个限定符的添加 */
		if (null != qualifiers) {
			for (String q : qualifiers) {
				column.setQualifier(Utils.wrap(q));
			}
		}
		columns.add(column);
		
		Map<ByteBuffer, ByteBuffer> attributes = new HashMap<ByteBuffer, ByteBuffer>();
		
		TScan tScan = new TScan();
		if (null != startRow && startRow.length >= 0) {
			tScan.setStartRow(startRow);
		}
		if (null != stopRow && stopRow.length >= 0) {
			tScan.setStopRow(stopRow);
		}
		tScan.setColumns(columns);
		tScan.setAttributes(attributes);
		/** 当设置过滤项时   设置过滤 */
		if (null != filters) {
			for (int i = 0; i < filters.length; i++) {//TODO 多filter时有问题
				tScan.filterString = Utils.wrap("ColumnPrefixFilter('" + filters[i] + "')");
			}
			filters = null;
		}
		return CLIENT.client.getScannerResults(table, tScan, Config.i.SCAN_SIZE);
	}//scan
	
	public List<TResult> scan(byte[] startRow, byte[] stopRow, ByteBuffer table, String family, String qualifier) throws TIOError, TException {
		List<String> qualifiers = new ArrayList<String>();
		qualifiers.add(qualifier);
		return scan(startRow, stopRow, table, family, qualifiers);
	}
	
	/**
	 * 封装Thrift Client的内部类 主要为了控制Client的重复实例化
	 */
	private class ClientSupport {
		
		private ClientSupport(String host, int port) {
			this.host = host.intern();
			this.port = port;
			TTransport transport = new TSocket(this.host, this.port);
			TProtocol protocol = new TBinaryProtocol(transport, true, true);
			this.client = new THBaseService.Client(protocol);
			try {
				transport.open();
			} catch (TTransportException e) {
				System.out.println("transport open is error.");
			}
		}
		
		private String host;
		private int port;
		private THBaseService.Iface client;
	}
}