package com.tensynchina.som.helper;

import java.util.Map;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;


public class JsonHelper {
	public static Map<String,Object> dealJsonObject(String jsonStr) throws Exception{
//		Map<String,Object> map=new HashMap<String,Object>();
//		JSONObject jsonObject = new JSONObject(jsonStr);
//		Iterator keyIter = jsonObject.keys();   
//        String key;   
//        Object value;  
//        while( keyIter.hasNext()){   
//            key = (String)keyIter.next();   
//            value = jsonObject.get(key);   
//            map.put(key, value);   
//        }
		ObjectMapper dom=new ObjectMapper();
		Map<String,Object> nodeMap = dom.readValue(jsonStr, new TypeReference<Map<String,Object>>(){});
		return nodeMap;
	}
}
