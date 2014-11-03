package com.secneo.hbase.utils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;

public class WriteFile {

	public static void write(final String content, final String path) {
		try {
			final File file = new File(path);
			File route = null;
			String systemName = System.getProperties().getProperty("os.name");
			if (systemName.startsWith("Windows") || systemName.startsWith("windows")) {
				if (path.lastIndexOf("\\") > 0) {
					route = new File(path.substring(0, path.lastIndexOf("\\")));
				}
			} else {
				if (path.lastIndexOf("/") > 0) {
					route = new File(path.substring(0, path.lastIndexOf("/")));
				}
			}
			if (null != route && !route.exists() && !route.isDirectory()) {
				route.mkdirs();
			}
			if (!file.exists()) {
				file.createNewFile();
			}
			final FileOutputStream fos = new FileOutputStream(path, true);
			final OutputStreamWriter osw = new OutputStreamWriter(fos, "UTF-8");
			osw.write(content);
			osw.flush();
			osw.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}//write
}