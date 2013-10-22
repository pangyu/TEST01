package com.tensynad.test;

import java.io.DataOutputStream;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;

import com.freeadb.commons.util.Utils;

public class test {
	
	
	
	public static String getData(){
		String str="";
		try {
			URL url = new URL("http://192.168.1.15:8983/solr/som_shard1_1_replica1/select?q=*%3A*&wt=json&indent=true");
			HttpURLConnection httpConn = (HttpURLConnection) url.openConnection();
			httpConn.setRequestProperty("Content-type", "text/xml");
			httpConn.setRequestProperty("Connection", "close");
//			httpConn.setRequestProperty("", "UTF-8");
			httpConn.setDoOutput(true);
			httpConn.setDoInput(true);
			httpConn.setRequestMethod("POST");
			httpConn.setInstanceFollowRedirects(true);
//			System.setProperty("sun.net.client.defaultConnectTimeout","45000");
//			System.setProperty("sun.net.client.defaultReadTimeout","45000");
			httpConn.setConnectTimeout(45000);
//			httpConn.setReadTimeout(60000);
			httpConn.connect();
			DataOutputStream out = new DataOutputStream(httpConn.getOutputStream());
//			out.writeBytes();
			System.out.println("AAAAAAAAAAAAA");
			String datas="<?xml version=\"1.0\" encoding=\"UTF-8\"?>";
			out.write(datas.getBytes("UTF-8"));
			out.flush();
			out.close();
			System.out.println("BBBBBBBBB");
			InputStream stream = httpConn.getInputStream();
			str = Utils.readStream(stream);
			System.out.println("CCCCCCCCCCCCCC");
//			log.debug("rs=" + str);
			stream.close();
			httpConn.disconnect();
		} catch (Exception e) {
			// TODO: handle exception
		}
		
		return str;
	}
	
	
	
	public static void main(String[] args) throws Exception{
		
		String str=getData();
		System.out.println("SS:"+str);
	}
}
