package com.tensynchina.som.core.config;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.util.log.Log;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import com.freeadb.commons.util.Utils;
import com.tensynchina.som.helper.CloudPrintWriter;
import com.tensynchina.som.helper.InsertHelper;
import com.tensynchina.som.helper.JsonHelper;

@Controller
@RequestMapping("/config")
public class ConfigController {
	private static String solrConfigBasePath = "/home/app/opt/solr-4.4";
	private static String solrConfigBaseDir = "multicore";
	private static CloudPrintWriter cpw=new CloudPrintWriter();
	@RequestMapping("/index")
	public String index(HttpServletRequest request ,HttpServletResponse response){
		request.setAttribute("message", "Hello,This is config test page!");
		return "index.jsp";
	}
	
	
	@RequestMapping("/collectionLists")
	@ResponseBody 
	public Map<String,Object> collectionLists(HttpServletRequest request ,HttpServletResponse response) throws IOException{
		Map<String,Object> map=new HashMap<String,Object>();
		InsertHelper ih=null;
		Object uid=null;
		try {
			
//			CollectionManager app= new CollectionManager();
//			app.connectServer();
			ih= InsertHelper.getInsertHelperInstance();
			uid = ih.getUid();
			CollectionManager app=ih.getCm(uid);
			Set<String> set=app.getCollections();
			map.put("status", "success");
			map.put("collections", set);
//			app.disconnectServer();
		} catch (Exception e) {
			map.put("status", "fail");
		}
		finally {
			if(uid!=null)
				ih.release(uid);
		}
		return map;
	}
	
	@RequestMapping("/collectionList")
	@ResponseBody 
	public Map<String,Object> collectionList(HttpServletRequest request ,HttpServletResponse response) throws IOException{
		Map<String,Object> map=new HashMap<String,Object>();
//		CollectionManager app= new CollectionManager();
		InsertHelper ih=null;
		Object uid=null;
		try {
//			app.connectServer();
			ih= InsertHelper.getInsertHelperInstance();
			uid = ih.getUid();
			CollectionManager app=ih.getCm(uid);
			Set<String> collections=app.getCollections();
			Map<String,Map<String,Object>> colMap=app.getCollectionStatus(collections);
			Map<String,String> liveNodesMap=app.getLiveNodes();
			map.put("status", "success");
			map.put("collections", colMap);
			map.put("liveNodes", liveNodesMap);
		} catch (Exception e) {
			map.put("status", "fail");
			e.printStackTrace(cpw);
			map.put("info", cpw.print());
		}finally {
			if(uid!=null)
				ih.release(uid);
		}
		
		return map;
	}
	
	@RequestMapping("/createCollection")
	@ResponseBody 
	public  Map<String,Object> createCollection(HttpServletRequest request ,HttpServletResponse response) throws IOException{
		Map<String,Object> map=new HashMap<String,Object>();
		InputStream is =request.getInputStream();
//		CollectionManager app= new CollectionManager();
		InsertHelper ih=null;
		Object uid=null;
		try {
//			app.connectServer();
			ih= InsertHelper.getInsertHelperInstance();
			uid = ih.getUid();
			CollectionManager app=ih.getCm(uid);
			String jsonStr=Utils.readStream(is);
			Map<String,Object> jsonMap=JsonHelper.dealJsonObject(jsonStr);
			String collectionName=jsonMap.get("collectionName").toString();
			int rf=2;
			int sn=2;
			int msp=2;
			String nodes="";
			if(jsonMap.containsKey("rf")) rf=Utils.parseInt(jsonMap.get("rf").toString(), 2);
			if(jsonMap.containsKey("sn")) rf=Utils.parseInt(jsonMap.get("sn").toString(), 2);
			if(jsonMap.containsKey("msp")) rf=Utils.parseInt(jsonMap.get("msp").toString(), 2);
			if(jsonMap.containsKey("nodes")) nodes=jsonMap.get("nodes").toString();
			String[] filesName=jsonMap.get("filesName").toString().split(",");
			for(String fileName:filesName){
				byte[] datas=readConfigFile(collectionName, fileName);
				app.uploadConfig(collectionName, fileName, datas);
			}
			
			app.createCollection(collectionName,rf,sn,msp,nodes);
			map.put("status", "success");
		} catch (Exception e) {
			e.printStackTrace(cpw);
			map.put("status", "fail");
			map.put("info", cpw.print());
		}finally {
			is.close();
			if(uid!=null)
				ih.release(uid);
		}
		return map;
	}
	
	public static byte[] readConfigFile(String collectionName,String fileName) throws Exception{
		Properties pp=Utils.getProperties();
		String configPath=pp.getProperty("configPath");
		configPath+="/"+collectionName+"/conf/"+fileName;
		byte[] buffer=null;
		try{
				URL url = new URL(configPath);
				HttpURLConnection httpConn = (HttpURLConnection) url.openConnection();
				httpConn.setRequestProperty("Content-type", "text/xml");
				httpConn.setRequestProperty("Connection", "close");
//				httpConn.setRequestProperty("", "UTF-8");
				httpConn.setDoOutput(true);
				httpConn.setDoInput(true);
				httpConn.setRequestMethod("POST");
				httpConn.setInstanceFollowRedirects(true);
				httpConn.setConnectTimeout(45000);
//				httpConn.setReadTimeout(60000);
				httpConn.connect();
				InputStream is = httpConn.getInputStream();
				ByteArrayOutputStream baos = new ByteArrayOutputStream();
				int len = 0;
				byte[] b = new byte[1024];
				while ((len = is.read(b, 0, b.length)) != -1) {                     
				    baos.write(b, 0, len);
				}
				buffer =  baos.toByteArray();
				is.close();
				httpConn.disconnect();

		}catch(Exception e){
			e.printStackTrace();
			throw e;
		}finally{
			
			
		}
		return buffer;
	}
	
	@RequestMapping("/delCollection")
	@ResponseBody 
	public  Map<String,Object> delCollection(HttpServletRequest request ,HttpServletResponse response) throws IOException{
		Map<String,Object> map=new HashMap<String,Object>();
		InputStream is =request.getInputStream();
//		CollectionManager app= new CollectionManager();
		InsertHelper ih=null;
		Object uid=null;
		try {
//			app.connectServer();
			ih= InsertHelper.getInsertHelperInstance();
			uid = ih.getUid();
			CollectionManager app=ih.getCm(uid);
			String jsonStr=Utils.readStream(is);
			Map<String,Object> jsonMap=JsonHelper.dealJsonObject(jsonStr);
			String collectionName=jsonMap.get("collectionName").toString();
			app.deleteCollection(collectionName);
			map.put("status", "success");
		} catch (Exception e) {
			e.printStackTrace(cpw);
			map.put("status", "fail");
			map.put("info", cpw.print());
		}finally {
			is.close();
			if(uid!=null)
				ih.release(uid);
		}
		return map;
	}
	
	
	@RequestMapping("/splitShard")
	@ResponseBody 
	public Map<String,Object> splitShard(HttpServletRequest request ,HttpServletResponse response) throws IOException{
		Map<String,Object> map=new HashMap<String,Object>();
		InputStream is =request.getInputStream();
//		CollectionManager app= new CollectionManager();
		InsertHelper ih=null;
		Object uid=null;
		try {
//			app.connectServer();
			ih= InsertHelper.getInsertHelperInstance();
			uid = ih.getUid();
			CollectionManager app=ih.getCm(uid);
			String jsonStr=Utils.readStream(is);
			Map<String,Object> jsonMap=JsonHelper.dealJsonObject(jsonStr);
			String collectionName=jsonMap.get("collectionName").toString();
			String shardName=jsonMap.get("shardName").toString();
			app.splitShard(collectionName, shardName);

			map.put("status", "success"); 
		} catch(Exception e){
			e.printStackTrace(cpw);
			map.put("status", "fail");
			map.put("info", cpw.print());
		}finally {
			is.close();
			if(uid!=null)
				ih.release(uid);
		}
		return map;
	}
	
	
	@RequestMapping("/delShard")
	@ResponseBody 
	public  Map<String,Object> delShard(HttpServletRequest request ,HttpServletResponse response) throws IOException{
		Map<String,Object> map=new HashMap<String,Object>();
		InputStream is =request.getInputStream();
//		CollectionManager app= new CollectionManager();
		InsertHelper ih=null;
		Object uid=null;
		try {
//			app.connectServer();
			ih= InsertHelper.getInsertHelperInstance();
			uid = ih.getUid();
			CollectionManager app=ih.getCm(uid);
			String jsonStr=Utils.readStream(is);
			Map<String,Object> jsonMap=JsonHelper.dealJsonObject(jsonStr);
			String collectionName=jsonMap.get("collectionName").toString();
			String shardName=jsonMap.get("shardName").toString();
			
			app.deleteShard(collectionName,shardName);
			map.put("status", "success");
		} catch (Exception e) {
			e.printStackTrace(cpw);
			map.put("status", "fail");
			map.put("info", cpw.print());
		}finally {
			is.close();
			if(uid!=null)
				ih.release(uid);
		}
		return map;
	}

	
	@RequestMapping("/createReplica")
	@ResponseBody 
	public  Map<String,Object> createReplica(HttpServletRequest request ,HttpServletResponse response) throws IOException{
		Map<String,Object> map=new HashMap<String,Object>();
		InputStream is =request.getInputStream();
//		CollectionManager app= new CollectionManager();
		InsertHelper ih=null;
		Object uid=null;
		try {
//			app.connectServer();
			ih= InsertHelper.getInsertHelperInstance();
			uid = ih.getUid();
			CollectionManager app=ih.getCm(uid);
			String jsonStr=Utils.readStream(is);
			Map<String,Object> jsonMap=JsonHelper.dealJsonObject(jsonStr);
			String collectionName=jsonMap.get("collectionName").toString();
			String replicaName=jsonMap.get("replicaName").toString();
			String shardName=jsonMap.get("shardName").toString();
			String solrBasePath=jsonMap.get("solrBasePath").toString();
			
			app.createReplica(replicaName, collectionName, shardName, solrBasePath);
			
			map.put("status", "success");
		} catch (Exception e) {
			e.printStackTrace(cpw);
			map.put("status", "fail");
			map.put("info", cpw.print());
		}finally {
			is.close();
			if(uid!=null)
				ih.release(uid);
		}
		return map;
	}
	
	@RequestMapping("/delReplica")
	@ResponseBody 
	public  Map<String,Object> delReplica(HttpServletRequest request ,HttpServletResponse response) throws IOException{
		Map<String,Object> map=new HashMap<String,Object>();
		InputStream is =request.getInputStream();
//		CollectionManager app= new CollectionManager();
		InsertHelper ih=null;
		Object uid=null;
		try {
//			app.connectServer();
			ih= InsertHelper.getInsertHelperInstance();
			uid = ih.getUid();
			CollectionManager app=ih.getCm(uid);
			String jsonStr=Utils.readStream(is);
			Map<String,Object> jsonMap=JsonHelper.dealJsonObject(jsonStr);
			
			String solrBasePath=jsonMap.get("solrBasePath").toString();
			String replicaName=jsonMap.get("replicaName").toString();
			app.deleteReplica(replicaName, solrBasePath);
			
			map.put("status", "success");
		} catch (Exception e) {
			e.printStackTrace(cpw);
			map.put("status", "fail");
			map.put("info", cpw.print());
		}finally {
			is.close();
			if(uid!=null)
				ih.release(uid);
		}
		return map;
	}
	
	

	public static void main(String[] args) throws Exception{
	}
}
