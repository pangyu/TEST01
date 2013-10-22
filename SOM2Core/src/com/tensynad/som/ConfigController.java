package com.tensynad.som;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.util.HashMap;
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
public class ConfigController {
	private static String solrConfigBasePath = "/home/app/opt/solr-4.4";
	private static String solrConfigBaseDir = "multicore";
	
	@RequestMapping("/index")
	public String index(HttpServletRequest request ,HttpServletResponse response){
		request.setAttribute("message", "Hello,This is config test page!");
		return "index.jsp";
	}
	
	@RequestMapping("/data")
	@ResponseBody 
	public Map<String,Object> collectionList(HttpServletRequest request ,HttpServletResponse response) throws IOException{
		Map<String,Object> map=new HashMap<String,Object>();
		InputStream is =request.getInputStream();
		CollectionManager app= new CollectionManager();
		app.connectServer();
		try {
			String jsonStr=Utils.readStream(is);
			Map<String,Object> jsonMap=JsonHelper.dealJsonObject(jsonStr);
			if(!jsonMap.containsKey("method") || Utils.isEmpty(jsonMap.get("method").toString())) {
				map.put("status", "fail");
				map.put("info", "lost method");
			}
			if(!jsonMap.containsKey("params")) {
				map.put("status", "fail");
				map.put("info", "lost params");
			}
			
			Object obj=configDispatcher(jsonMap.get("method").toString(),jsonMap.get("params"));
			map.put("data", obj);
		} catch (Exception e) {
			map.put("status", "fail");
			map.put("info", e.getMessage());
		} finally{
			is.close();
			app.disconnectServer();
		}
		
		return map;
	}
	
	private Object configDispatcher(String method,Object params) throws Exception{
		Object obj= null;
		Object o =Class.forName("com.solrj.CollectionManager").newInstance();
		try {
			invoke(o,"connectServer",null,null);
			if(params==null || Utils.isEmpty(params.toString())){
				obj=invoke(o,method,null,null);
			}else{
				String[] temp=params.toString().split(",");
				obj=this.invoke(o, method, new Class[]{String.class},temp);
			}
		} catch (Exception e) {
			System.out.println("exec method error:"+e.getMessage());
		}finally{
			this.invoke(o,"disconnectServer",null,null);
		}
		
		
		
		return obj;
	}
	
	
	private Map<String,Object> collectionList(CollectionManager app,Object params) throws Exception{
		Map<String,Object> map=new HashMap<String,Object>();
		try {
			Set<String> set=app.getCollections();
			map.put("status", "success");
			map.put("collections", set);
		} catch (Exception e) {
			map.put("status", "fail");
		}
		return map;
	}


	
	
	protected static Object invoke(Object o,String methodName,Class[] types,Object[] params)throws java.lang.Exception{
		try {
			Method m = o.getClass().getMethod(methodName, types);
			return m.invoke(o, params);
		} catch (Exception e) {
			System.out.println(methodName+" Invoke Error."+e);

			throw e;
		} 
		
		
//		return null;
	}
	public static void main(String[] args) throws Exception{
		Object o =Class.forName("com.solrj.CollectionManager").newInstance();
		String methodName="getCollections";
		invoke(o,"connectServer",null,null);
		String[] temp={"som"};
		Class[] cs={String.class};
		Object obj=invoke(o,"getShards",cs,temp);
		invoke(o,"disconnectServer",null,null);
		String params="";
		
//		Method m = o.getClass().getMethod(methodName, null);
//		Object obj=m.invoke(o, null);
		Map<String,Map<String,Object>> mm=(Map<String,Map<String,Object>>)obj;
		System.out.println("OOOOOOOOOOO:"+obj.getClass());
		for(String key:mm.keySet()){
			System.out.println("KKKKKKKKKKKKK:"+key);
		}
	}
}
