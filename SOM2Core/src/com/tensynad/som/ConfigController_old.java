package com.tensynad.som;

import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import com.freeadb.commons.util.Utils;
import com.tensynad.test.CollectionManager;
import com.tensynchina.som.helper.JsonHelper;



@Controller
@RequestMapping("/config")
public class ConfigController_old {
	private static String solrConfigBasePath = "/home/app/opt/solr-4.4";
	private static String solrConfigBaseDir = "multicore";
	
	
	@RequestMapping("/uploadFile")
	@ResponseBody
	public Map<String,Object> uploadFile(HttpServletRequest request ,HttpServletResponse response) throws IOException{
		org.springframework.http.converter.json.MappingJacksonHttpMessageConverter a;
		Map<String,Object> map=new HashMap<String,Object>();
		InputStream is =request.getInputStream();
		try {
			String jsonStr=Utils.readStream(is);
			Map<String,Object> jsonMap=JsonHelper.dealJsonObject(jsonStr);
			if(!jsonMap.containsKey("configName")) {
				map.put("status", "fail"); 
				map.put("info", "configName is null."); 
				return map;
			}
			if(!jsonMap.containsKey("configData")) {
				map.put("status", "fail"); 
				map.put("info", "configData is null."); 
				return map;
			}
			String configName=jsonMap.get("configName").toString();
			String configData=jsonMap.get("configName").toString();
			solrConfigBasePath="."+ File.separator+"solrconfig";
			String configFilePath =solrConfigBasePath + File.separator + solrConfigBaseDir+ File.separator+"py"+ File.separator+"conf";
			createFolder(configFilePath);
			configName=configFilePath+File.separator+configName;
			writeFile(configData,configName);
			map.put("statuc", "success");
		} catch (Exception e) {
			map.put("statuc", "fail");
			map.put("info", e.getMessage());
		}
		
		return map;
	}
	
	private static void writeFile(String configData,String configDataname) throws Exception{
		FileOutputStream outSTr = null;
		BufferedOutputStream Buff = null;
		try {
			byte[] bs = configData.getBytes("utf-8");
			outSTr = new FileOutputStream(new File(configDataname));
			Buff = new BufferedOutputStream(outSTr);
			Buff.write(bs);
			Buff.flush();
			Buff.close();
		} catch (Exception e) {
			// TODO: handle exception
		}
		
	}
	
	
	private static boolean createFolder(String path){
		File f = new File(path);
		boolean flag =true;
		if(!f.exists()){
			flag=f.mkdirs();
		}
		return flag;
	}
	@RequestMapping("/index")
	public String index(HttpServletRequest request ,HttpServletResponse response){
		request.setAttribute("message", "Hello,This is a example of Spring3 RESTful!");
		return "index.jsp";
	}
	
	
	@RequestMapping("/collectionList")
	@ResponseBody 
	public Map<String,Object> collectionList(HttpServletRequest request ,HttpServletResponse response) throws IOException{
		Map<String,Object> map=new HashMap<String,Object>();
		try {
			CollectionManager app= new CollectionManager();
			app.connectServer();
			Set<String> set=app.getCollections();
			map.put("status", "success");
			map.put("collections", set);
			app.disconnectServer();
		} catch (Exception e) {
			map.put("status", "fail");
		}
		return map;
	}
	
	@RequestMapping("/createCollection")
	@ResponseBody 
	public  Map<String,Object> createCollection(HttpServletRequest request ,HttpServletResponse response) throws IOException{
		Map<String,Object> map=new HashMap<String,Object>();
		InputStream is =request.getInputStream();
		try {
			String jsonStr=Utils.readStream(is);
			Map<String,Object> jsonMap=JsonHelper.dealJsonObject(jsonStr);
			CollectionManager app= new CollectionManager();
			if(!jsonMap.containsKey("collectionName")) {
				map.put("status", "fail"); 
				map.put("info", "collectionName is null."); 
				return map;
			}
			String collectionName=jsonMap.get("collectionName").toString();
			int rf=2;
			int sn=2;
			int msp=2;
			if(jsonMap.containsKey("rf")) rf=Utils.parseInt(jsonMap.get("rf").toString(), 2);
			if(jsonMap.containsKey("sn")) rf=Utils.parseInt(jsonMap.get("sn").toString(), 2);
			if(jsonMap.containsKey("msp")) rf=Utils.parseInt(jsonMap.get("msp").toString(), 2);
			solrConfigBasePath="."+ File.separator+"solrconfig";
			String solrLocalConfigPath=solrConfigBasePath + File.separator + solrConfigBaseDir + File.separator;
			app.connectServer();
			app.uploadConfig(solrLocalConfigPath,collectionName);
			app.createCollection(collectionName,rf,sn,msp,null);
			app.disconnectServer();
			System.out.println("INFO: collectionName:"+collectionName+" \t rf:"+rf+" \t sn:"+sn+" \t smp:"+msp);
			System.out.println("IIII:"+jsonStr);
			map.put("status", "success");
		} catch (Exception e) {
			System.out.println("error:"+e);
			map.put("status", "fail");
			map.put("info", e.getMessage());
		}
		is.close();
		return map;
	}
	
	@RequestMapping("/delCollection")
	@ResponseBody 
	public  Map<String,Object> delCollection(HttpServletRequest request ,HttpServletResponse response) throws IOException{
		Map<String,Object> map=new HashMap<String,Object>();
		InputStream is =request.getInputStream();
		try {
			String jsonStr=Utils.readStream(is);
			Map<String,Object> jsonMap=JsonHelper.dealJsonObject(jsonStr);
			CollectionManager app= new CollectionManager();
			if(!jsonMap.containsKey("collectionName")) {
				map.put("status", "fail"); 
				map.put("info", "collectionName is null."); 
				return map;
			}
			String collectionName=jsonMap.get("collectionName").toString();
			app.connectServer();
			app.deleteCollection(collectionName);
			app.disconnectServer();
			map.put("status", "success");
		} catch (Exception e) {
			System.out.println("error:"+e);
			map.put("status", "fail");
			map.put("info", e.getMessage());
		}
		is.close();
		return map;
	}
	
	@RequestMapping("/shardList")
	@ResponseBody 
	public Map<String,Object> shardList(HttpServletRequest request ,HttpServletResponse response) throws IOException{
		Map<String,Object> map=new HashMap<String,Object>();
		InputStream is =request.getInputStream();
		try {
			String jsonStr=Utils.readStream(is);
			Map<String,Object> jsonMap=JsonHelper.dealJsonObject(jsonStr);
			CollectionManager app= new CollectionManager();
			if(!jsonMap.containsKey("collectionName")) {
				map.put("status", "fail"); 
				map.put("info", "collectionName is null."); 
				return map;
			}
			String collectionName=jsonMap.get("collectionName").toString();
			app.connectServer();
			Map<String,Map<String,Object>> shardMap=app.getShards(collectionName);
			List<String> shardList=new ArrayList<String>();
			String shardName="";
			for(String key:shardMap.keySet()){
				shardName=shardMap.get(key).get("shard_name").toString();
				if(!shardList.contains(shardName) && !Utils.isEmpty(shardName)){
					shardList.add(shardName);
				}
			}
			app.disconnectServer();
			map.put("status", "success"); 
			map.put("shards", shardList);
		} catch(Exception e){
			System.out.println("error:"+e);
			map.put("status", "fail");
			map.put("info", e.getMessage());
			
		}
		return map;
	}
	
	@RequestMapping("/splitShard")
	@ResponseBody 
	public Map<String,Object> splitShard(HttpServletRequest request ,HttpServletResponse response) throws IOException{
		Map<String,Object> map=new HashMap<String,Object>();
		InputStream is =request.getInputStream();
		try {
			String jsonStr=Utils.readStream(is);
			Map<String,Object> jsonMap=JsonHelper.dealJsonObject(jsonStr);
			CollectionManager app= new CollectionManager();
			if(!jsonMap.containsKey("collectionName")) {
				map.put("status", "fail"); 
				map.put("info", "collectionName is null."); 
				return map;
			}
			if(!jsonMap.containsKey("shardName")) {
				map.put("status", "fail"); 
				map.put("info", "shardName is null."); 
				return map;
			}
			String collectionName=jsonMap.get("collectionName").toString();
			String shardName=jsonMap.get("shardName").toString();
			app.connectServer();
			
			app.splitShard(collectionName, shardName);
			app.disconnectServer();
			map.put("status", "success"); 
		} catch(Exception e){
			System.out.println("error:"+e);
			map.put("status", "fail");
			map.put("info", e.getMessage());
			
		}
		return map;
	}
	
	
	@RequestMapping("/delShard")
	@ResponseBody 
	public  Map<String,Object> delShard(HttpServletRequest request ,HttpServletResponse response) throws IOException{
		Map<String,Object> map=new HashMap<String,Object>();
		InputStream is =request.getInputStream();
		try {
			String jsonStr=Utils.readStream(is);
			Map<String,Object> jsonMap=JsonHelper.dealJsonObject(jsonStr);
			CollectionManager app= new CollectionManager();
			if(!jsonMap.containsKey("collectionName")) {
				map.put("status", "fail"); 
				map.put("info", "collectionName is null."); 
				return map;
			}
			if(!jsonMap.containsKey("shardName")) {
				map.put("status", "fail"); 
				map.put("info", "shardName is null."); 
				return map;
			}
			String collectionName=jsonMap.get("collectionName").toString();
			String shardName=jsonMap.get("shardName").toString();
			app.connectServer();
			app.deleteShard(collectionName,shardName);
			app.disconnectServer();
			map.put("status", "success");
		} catch (Exception e) {
			System.out.println("error:"+e);
			map.put("status", "fail");
			map.put("info", e.getMessage());
		}
		is.close();
		return map;
	}
	
	@RequestMapping("/dataBlockList")
	@ResponseBody 
	public Map<String,Object> dataBlockList(HttpServletRequest request ,HttpServletResponse response) throws IOException{
		Map<String,Object> map=new HashMap<String,Object>();
		InputStream is =request.getInputStream();
		try {
			String jsonStr=Utils.readStream(is);
			Map<String,Object> jsonMap=JsonHelper.dealJsonObject(jsonStr);
			CollectionManager app= new CollectionManager();
			if(!jsonMap.containsKey("collectionName")) {
				map.put("status", "fail"); 
				map.put("info", "collectionName is null."); 
				return map;
			}
			if(!jsonMap.containsKey("shardName")) {
				map.put("status", "fail"); 
				map.put("info", "shardName is null."); 
				return map;
			}
			String collectionName=jsonMap.get("collectionName").toString();
			String shardName=jsonMap.get("shardName").toString();
//			String dataBlockName="";
			app.connectServer();
			Map<String,Map<String,Object>> shardMap=app.getShards(collectionName);
			List<String> dataBlockList=new ArrayList<String>();
			for(String key:shardMap.keySet()){
//				System.out.println("SSS:"+shardName+"T \t AAAAAAAAA:"+shardMap.get(key).get("shard_name").toString()+"T");
				if(shardName.equals(shardMap.get(key).get("shard_name").toString())){
//					System.out.println("BBBBBBBBBBBBBBBB");
					if(!dataBlockList.contains(key) && !Utils.isEmpty(key)){
						dataBlockList.add(key);
					}
				}
			}
			app.disconnectServer();
			map.put("status", "success"); 
			map.put("dataBlocks", dataBlockList);
		} catch(Exception e){
			System.out.println("error:"+e);
			map.put("status", "fail");
			map.put("info", e.getMessage());
			
		}
		return map;
	}
	
	@RequestMapping("/createDataBlock")
	@ResponseBody 
	public  Map<String,Object> createDataBlock(HttpServletRequest request ,HttpServletResponse response) throws IOException{
		Map<String,Object> map=new HashMap<String,Object>();
		InputStream is =request.getInputStream();
		try {
			String jsonStr=Utils.readStream(is);
			Map<String,Object> jsonMap=JsonHelper.dealJsonObject(jsonStr);
			CollectionManager app= new CollectionManager();
			if(!jsonMap.containsKey("collectionName")) {
				map.put("status", "fail"); 
				map.put("info", "collectionName is null."); 
				return map;
			}
			String collectionName=jsonMap.get("collectionName").toString();
			int rf=2;
			int sn=2;
			int msp=2;
			if(jsonMap.containsKey("rf")) rf=Utils.parseInt(jsonMap.get("rf").toString(), 2);
			if(jsonMap.containsKey("sn")) rf=Utils.parseInt(jsonMap.get("sn").toString(), 2);
			if(jsonMap.containsKey("msp")) rf=Utils.parseInt(jsonMap.get("msp").toString(), 2);
			solrConfigBasePath="."+ File.separator+"solrconfig";
			String solrLocalConfigPath=solrConfigBasePath + File.separator + solrConfigBaseDir + File.separator;
			app.connectServer();
//			app.uploadConfig(solrLocalConfigPath,collectionName);
//			app.createCollection(collectionName,rf,sn,msp,null);
//			app.createReplica(replicaName, collectionName, shardName, solrBasePath)
			app.disconnectServer();
			System.out.println("INFO: collectionName:"+collectionName+" \t rf:"+rf+" \t sn:"+sn+" \t smp:"+msp);
			System.out.println("IIII:"+jsonStr);
			map.put("status", "success");
		} catch (Exception e) {
			System.out.println("error:"+e);
			map.put("status", "fail");
			map.put("info", e.getMessage());
		}
		is.close();
		return map;
	}




	public static void main(String[] args) throws Exception{
		createFolder("D:\\test\\test\\test");
		String path="D:\\test\\test\\test\\test.txt";
		File file=new File(path);
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file)));
		bw.write("something");
		bw.close();
		System.out.println("PPPPPPPP:"+file.getPath());
	}
}
