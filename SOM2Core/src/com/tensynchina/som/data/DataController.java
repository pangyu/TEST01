package com.tensynchina.som.data;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.solr.common.SolrInputDocument;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import com.freeadb.commons.util.Utils;
import com.tensynad.common.util.ByteBuffer;
import com.tensynchina.som.core.config.CollectionManager;
import com.tensynchina.som.helper.CloudPrintWriter;
import com.tensynchina.som.helper.InsertHelper;
import com.tensynchina.som.helper.JsonHelper;


@Controller
@RequestMapping("/data")
public class DataController{
	private static CloudPrintWriter cpw=new CloudPrintWriter();
	private static Log log = LogFactory.getLog(DataController.class);
	@RequestMapping("/insert")
	@ResponseBody 
	public  Map<String,Object> insert(HttpServletRequest request ,HttpServletResponse response) throws IOException{
		Map<String,Object> map=new HashMap<String,Object>();
		InputStream is =request.getInputStream();
		InsertHelper ih=null;
		Object uid=null;
		String jsonStr="";
		try {
			ih= InsertHelper.getInsertHelperInstance();
			uid = ih.getUid();
//			jsonStr=Utils.readStream(is);
			jsonStr=readInputStream(is);
			Map<String,Object> nodeMap=JsonHelper.dealJsonObject(jsonStr);
			ArrayList<SolrInputDocument> docs=new ArrayList<SolrInputDocument>();
			String collectionName=nodeMap.get("collectionName").toString();
//			JSONArray jas=(JSONArray)jsonMap.get("datas");
//			ObjectMapper dom=new ObjectMapper();
//			Map<String,Object> nodeMap = dom.readValue(jsonStr, new TypeReference<Map<String,Object>>(){});
			
//			for(String key:nodeMap.keySet()){
//				System.out.println("obj k:"+key+",v:"+nodeMap.get(key)+",clazz:"+nodeMap.get(key).getClass());
//			}
			List datas = (List)nodeMap.get("datas");
			String regex="^((((1[6-9]|[2-9]\\d)\\d{2})-(0?[13578]|1[02])-(0?[1-9]|[12]\\d|3[01]))|(((1[6-9]|[2-9]\\d)\\d{2})-(0?[13456789]|1[012])-(0?[1-9]|[12]\\d|30))|(((1[6-9]|[2-9]\\d)\\d{2})-0?2-(0?[1-9]|1\\d|2[0-8]))|(((1[6-9]|[2-9]\\d)(0[48]|[2468][048]|[13579][26])|((16|[2468][048]|[3579][26])00))-0?2-29-)) (20|21|22|23|[0-1]?\\d):[0-5]?\\d:[0-5]?\\d$";
			regex="^(\\d{4}-\\d{2}-\\d{2}\\s\\d{2}:\\d{2}:\\d{2})";
			for(int i =0;i<datas.size();i++){
				Map<String,Object> dmap = (Map<String,Object>)datas.get(i);
				SolrInputDocument doc=new SolrInputDocument();
				for(String key:dmap.keySet()){
					Object v = dmap.get(key);
					if(v instanceof String){
						String sv = (String)v;
						if(sv.matches(regex)){
							v = Utils.toDate(sv, "yyyy-MM-dd HH:mm:ss");
						}
					}
					doc.addField(key, v);
//					System.out.println("k:"+key+",v:"+v);
				}
				docs.add(doc);
			}
			
			
//			for(int i=0;i<jas.length();i++){
//				JSONObject jb=(JSONObject)jas.get(i);
//				SolrInputDocument doc=new SolrInputDocument();
//				for(Object key:jb.keySet()){
//					doc.addField(key+"", jb.get(""+key));
//				}
//				docs.add(doc);
//			}
			ih.addDocuments(docs, collectionName,uid);
			map.put("status", "success");
		} catch (Exception e) {
			e.printStackTrace(cpw);
			map.put("status", "fail");
			map.put("info", cpw.print());
			log.error("error jsonStr:"+jsonStr);
		}finally{
			is.close();
			if(ih!=null&&uid!=null) ih.release(uid);
		}
		return map;
	}
	
	@RequestMapping(value="/{collectionName}/s{value}",method=RequestMethod.GET)
	@ResponseBody 
	public  String query(@PathVariable String collectionName,@PathVariable String value,HttpServletRequest request ,HttpServletResponse response) throws IOException{
//		Map<String,Object> map=new HashMap<String,Object>();
		CollectionManager app= new CollectionManager();
//		map.put("status", "success");
		String json="{\"status\":\"success\"}";
		try {
			app.connectServer();
			json = app.query(request,collectionName);
//			map.put("info", json);
		} catch (Exception e){
			e.printStackTrace(cpw);
//			map.put("status", "fail");
//			map.put("info", cpw.print());
			json="{\"status\":\"fail\",\"info\":\""+cpw.print()+"\"}";
		} finally {
			app.disconnectServer();
		}
		return json;
	}
	@RequestMapping(value="/del/{collectionName}",method=RequestMethod.GET)
	@ResponseBody 
	public  Map<String,Object> deldatas(@PathVariable String collectionName,HttpServletRequest request ,HttpServletResponse response) throws IOException{
		Map<String,Object> map=new HashMap<String,Object>();
//		CollectionManager app= new CollectionManager();
		InsertHelper ih=null;
		Object uid=null;

		try {
//			app.connectServer();、
			ih= InsertHelper.getInsertHelperInstance();
			uid = ih.getUid();
			CollectionManager app=ih.getCm(uid);
			app.deleteAllDocuments(collectionName);
			ih.resetCounter(collectionName);
			map.put("status", "success");
		} catch (Exception e){
			e.printStackTrace(cpw);
			map.put("status", "fail");
			map.put("info", cpw.print());
//			json="{\"status\":\"fail\",\"info\":\""+cpw.print()+"\"}";
		} finally {
//			app.disconnectServer();
			if(uid!=null)
				ih.release(uid);
		}
		return map;
	}
	
	@RequestMapping("/show")
	@ResponseBody 
	public  Map<String,Object> show(HttpServletRequest request ,HttpServletResponse response) throws IOException{
		Map<String,Object> map=new HashMap<String,Object>();
		map.put("status", "success");
		return map;
	}
	
	public static void main(String[] args) throws Exception{
//		SolrInputDocument doc=new SolrInputDocument();
//		ArrayList<SolrInputDocument> docs=new ArrayList<SolrInputDocument>();
//		doc.addField("id", "3");
//		doc.addField("subject", "再试一次 100");
//		doc.addField("url", "http://www.baidu.com/q=600");
//		doc.addField(
//				"content",
//				"1 11月3日以来，截至9月14日，QDII基金全体实现正收益，平均涨幅达到6.21%。其中，涨幅最高的是上投全球天然资源股票，为15.34%；其次是华宝油气、富国全球顶级消费品股票，分别上涨14.59%和12.31%。还有招商全球资源股票、华安标普石油指数、交银全球资源股票、汇添富黄金及贵金属等涨幅都在10%以上。易方达黄金主题，诺安全球黄金的涨幅也很可观");
//		
//		docs.add(doc);
//		CollectionManager app=new CollectionManager();
//		app.connectServer();
////		app.addDocuments(docs,"som_test");
////		app.commitDocuments();
//		app.deleteAllDocuments("som_test");
//		app.disconnectServer();
//		String regex="^((((1[6-9]|[2-9]\\d)\\d{2})-(0?[13578]|1[02])-(0?[1-9]|[12]\\d|3[01]))|(((1[6-9]|[2-9]\\d)\\d{2})-(0?[13456789]|1[012])-(0?[1-9]|[12]\\d|30))|(((1[6-9]|[2-9]\\d)\\d{2})-0?2-(0?[1-9]|1\\d|2[0-8]))|(((1[6-9]|[2-9]\\d)(0[48]|[2468][048]|[13579][26])|((16|[2468][048]|[3579][26])00))-0?2-29-)) (20|21|22|23|[0-1]?\\d):[0-5]?\\d:[0-5]?\\d$";
//		
//		regex="^(\\d{4}-\\d{2}-\\d{2}\\s\\d{2}:\\d{2}:\\d{2})";
//		String str="2000-02-29  00:00:00";
//		System.out.println("eeeeeeeeee:"+str.matches(regex));
		
//		Map<String ,Long> mm=new HashMap<String,Long>();
//		mm.put("aa", 2l);
//		Long ll=mm.get("aa");
//		mm.put("aa", ll);
//		System.out.print("DD:"+mm.get("aa"));
		String path="D:\\opt_logs.log";
		String str=readInputStream(new FileInputStream(new File(path)));
		System.out.println("sss:"+str);
		Map<String,Object> nodeMap=JsonHelper.dealJsonObject(str);	
		System.out.println("MM:"+nodeMap);
		List datas = (List)nodeMap.get("datas");
		for(int i =0;i<datas.size();i++){
			Map<String,Object> dmap = (Map<String,Object>)datas.get(i);
			for(String key:dmap.keySet()){
				Object v = dmap.get(key);
				System.out.println("k:"+key+",v:"+v);
			}
		}
		
	}
	public static Map<String,Object> deal(String str){
		Map<String,Object> map=null;
		try {
			map=JsonHelper.dealJsonObject(str);	
		} catch (Exception e) {
			// TODO: handle exception
			System.out.println("eeee:"+e.getMessage());
		}
		return map;
	}
	
	public static String readInputStream(InputStream is){
		String info="";
		ByteBuffer bb =new ByteBuffer(128);
		int[] skipChar={0};
		try {   
			int tempchar;   
			while ((tempchar = is.read()) != -1){   
				boolean flag=true;
				for(int i=0;i<skipChar.length;i++){
					if(skipChar[i]==tempchar) {
						flag=false;
						break;
					}
				}
				if (flag){   
					bb.append(tempchar);
				}   
			}   
			
		} catch (Exception e) {   
			log.error("read file error:", e);
		} finally{
		}
		info=bb.toString("UTF-8");
		return info;
	}
	
	
	public static String readFile(File file){
		String info="";
		Reader reader = null;   
		try {   
//			 一次读一个字符   
			reader = new InputStreamReader(new FileInputStream(file));   
			int tempchar;   
			while ((tempchar = reader.read()) != -1){   
//			对于windows下，rn这两个字符在一起时，表示一个换行。   
//			但如果这两个字符分开显示时，会换两次行。   
//			因此，屏蔽掉r，或者屏蔽n。否则，将会多出很多空行。   
				if (((char)tempchar) != '\r' && tempchar!=0){   
					info+=(char)tempchar;
					System.out.println("AAAAAAAAAA:\t"+(char)tempchar+":\t"+tempchar);
				}   
			}   
			reader.close();   
		} catch (Exception e) {   
			log.error("read file error:", e);
		} 
		return info;
	}
}
