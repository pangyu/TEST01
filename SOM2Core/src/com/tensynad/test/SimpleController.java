package com.tensynad.test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

//import org.json.JSONObject;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import com.freeadb.commons.util.Utils;
import com.tensynchina.som.helper.JsonHelper;


@Controller
@RequestMapping("/simple")
public class SimpleController {
	private static String solrConfigBasePath = "/home/app/opt/solr-4.4";
	private static String solrConfigBaseDir = "multicore";
	@RequestMapping("/index")
	public String index(HttpServletRequest request ,HttpServletResponse response){
		request.setAttribute("message", "Hello,This is a example of Spring3 RESTful!");
		return "index.jsp";
	}
	
	@RequestMapping(value="/{id}",method=RequestMethod.GET)
	public String get(@PathVariable String id,HttpServletRequest request ,HttpServletResponse response) throws IOException{
		//request.setAttribute("message", "Hello,This is a example of Spring3 RESTful!<br/>ID:"+id+"");
		response.getWriter().write("Method Get:You put id is : "+id);
		System.out.println("Method is GET");
		//return "index.jsp";
		return null;
	}
	@RequestMapping(value="/{id}",method=RequestMethod.POST)
	public String create(@PathVariable String id,HttpServletRequest request ,HttpServletResponse response) throws IOException{
		response.getWriter().write("Method Post:You put id is : "+id);
		System.out.println("Method is POST");
		//return "index.jsp";
		return null;
	}
	@RequestMapping("/add")
	@ResponseBody 
	public  Map<String,Object> add(HttpServletRequest request ,HttpServletResponse response) throws IOException{
//		String data=request.getAttribute("data").toString();
//		response.getWriter().write("Method Post:You put id is : "+data);
//		System.out.println("Method is POST:"+data);
		//return "index.jsp";
		Map<String,Object> map=new HashMap<String,Object>();
		InputStream is =request.getInputStream();
		try {
			String jsonStr=Utils.readStream(is);
			Map<String,Object> jsonMap=JsonHelper.dealJsonObject(jsonStr);
			CollectionManager app= new CollectionManager();
			if(!jsonMap.containsKey("name")) {
				map.put("status", "fail"); 
				map.put("info", "collectionName is null."); 
				return map;
			}
			String collectionName=jsonMap.get("name").toString();
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
			app.disconnectServer();
			System.out.println("INFO: name:"+collectionName+" \t rf:"+rf+" \t sn:"+sn+" \t smp:"+msp);
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
	
	 public static String changeCharset(String str, String oldCharset, String newCharset)
			   throws UnsupportedEncodingException {
		  if (str != null) {
		   //用旧的字符编码解码字符串。解码可能会出现异常。
		   byte[] bs = str.getBytes(oldCharset);
		   //用新的字符编码生成字符串
		   return new String(bs, newCharset);
		  }
		  return null;
		 }
	 
	
	@RequestMapping("/list")
	@ResponseBody 
	public Map<String,Object> list(HttpServletRequest request ,HttpServletResponse response) throws IOException{
		Map<String,Object> map=new HashMap<String,Object>();
		
//		return new HashMap<String,Object>().put("success",true);
		map.put("success", "ok");
		CollectionManager app= new CollectionManager();
		app.connectServer();
		Set<String> set=app.getCollections();
		app.disconnectServer();
		map.put("collections", set);
		map.put("name", "庞羽");
//		response.getWriter().write("callback({\"name\":\"b\"})");
		return map;
	}
	
	
//	private Map<String,Object> dealJsonObject(String jsonStr){
//		Map<String,Object> map=new HashMap<String,Object>();
//		JSONObject jsonObject = new JSONObject(jsonStr);
//		Iterator  keyIter = jsonObject.keys();   
//        String key;   
//        Object value;  
//        while( keyIter.hasNext()){   
//            key = (String)keyIter.next();   
//            value = jsonObject.get(key);   
//            map.put(key, value);   
//        }  
//		return map;
//	}
	public static void main(String[] args) throws Exception{
		String jsonString="{\"name\":\"庞羽\",\"rf\":\"2\",\"sn\":\"2\",\"msp\":\"2\"}";
//		JSONArray array;
//        array = new JSONArray(s);
//        StringBuilder sb = new StringBuilder("\r\r----------------------\r");
//        for (int i = 0; i < array.length(); i++) {
//            JSONObject obj = array.getJSONObject(i);
////            sb.append("id:").append(obj.getInt("id")).append("\r");
//            sb.append("name:").append(obj.getString("name")).append("\r");
//            sb.append("rf:").append(obj.getString("rf")).append("\r");
//            sb.append("sn:").append(obj.getString("sn")).append("\r");
//            sb.append("----------------------\r");
//        }
//        System.out.println(sb.toString());

	}
}
