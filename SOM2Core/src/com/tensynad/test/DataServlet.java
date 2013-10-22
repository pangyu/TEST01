package com.tensynad.test;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.URL;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.freeadb.commons.util.Utils;


public class DataServlet extends HttpServlet {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private static Log log = LogFactory.getLog(DataServlet.class);
	private static String serverAddr = "http://192.168.1.15:8983/solr/som_shard1_1_replica1/select?q=*%3A*&wt=json&indent=true";
	protected void doPost(HttpServletRequest req, HttpServletResponse res)
			throws ServletException, IOException {
		System.out.println("AAAAAAAAAAAAAAAAAAAAAAAAAA");
		String xml="<?xml version=\"1.0\" encoding=\"UTF-8\"?><root><data>";
		try {
//			String str=getData();
//			System.out.println("SSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSS:"+str);
//			StringBuffer sb=new StringBuffer();
//			res.setContentType("text/plain;charset=UTF-8");
//			xml+=str+"</data></root>";  
//			res.getWriter().write(str);
			
			res.setContentType("application/json;charset=UTF-8");
			PrintWriter out = res.getWriter();
	        String data = "{\"list\":[{\"name\":\"胡阳\",\"age\":\"24\"},{\"name\":\"胡阳\",\"age\":\"23\"}]}";//"{\"options\":\"[{\"name\":\"胡阳\",\"age\":\"24\"},{\"name\":\"胡阳\",\"age\":\"23\"}]\"}";//构建的json数据
//	        {"options":"[{\"text\":\"太仓\",\"value\":\"1\"},{\"text\":\"昆山\",\"value\":\"2\"},{\"text\":\"苏州\",\"value\":\"3\"}]"}
	        out.println(data);
	        out.close();
		} catch (Exception e) {
			log.error("error:", e);
		}
	}
	
	protected void doGet(HttpServletRequest req, HttpServletResponse res)
			throws ServletException, IOException {
		
		doPost(req, res);
	}
	
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
//			out.writeBytes(datas);
			String datas="";
			out.write(datas.getBytes("UTF-8"));
			out.flush();
			out.close();
			InputStream stream = httpConn.getInputStream();
			str = Utils.readStream(stream);
			log.debug("rs=" + str);
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
