package com.tensynchina.som.helper;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.solr.common.SolrInputDocument;

import com.tensynchina.som.core.config.CollectionManager;


public class InsertHelper extends Thread{
	private static Log log = LogFactory.getLog(InsertHelper.class);
	protected static InsertHelper ih;
	private CollectionManager[] cms;
	private static Map<CollectionManager,Integer> retain=new HashMap<CollectionManager,Integer>();
	private long docCount=0;
	private static int pos=0;
	private Map<CollectionManager,Integer> status=new HashMap<CollectionManager,Integer>();
	private static final long maxCount=10000;
	private static final long execCount=200;
	Map<String,Long>  dataMap= new HashMap<String,Long>();
	Map<String,Long>  cndMap=new HashMap<String,Long>();
	private InsertHelper() throws Exception{
		cms=new CollectionManager[2];
		cms[0]=new CollectionManager();
		cms[1]=new CollectionManager();
		retain.put(cms[0], 0);
		retain.put(cms[1], 0);
		status.put(cms[0], 0);
		status.put(cms[1], 0);
		this.setDaemon(true);
		this.start();
	}
	public static InsertHelper getInsertHelperInstance()throws Exception{
		synchronized(InsertHelper.class){
			if(ih==null)
				ih=new InsertHelper();
		}
		return ih;
	}
	public Object getUid(){
//		return cms[pos];
		return conn();
	}
	public CollectionManager getCm(Object uid){
		return (CollectionManager)uid;
	}
	private CollectionManager conn(){
		boolean ret=false;
		CollectionManager cm = null;
		try {
			for(int i=0;i<cms.length;i++){
				if(getStatus(cms[i])==1){
					pos=i;
					cm = cms[i];
					ret=true;
					break;
				}
			}
			if(!ret){
				for(int i=0;i<cms.length;i++){
					if(getStatus(cms[i])==0){
						pos=i;
						cms[i].connectServer();
						cm = cms[i];
						this.setStatus(cm, 1);
						ret=true;
						break;
					}
				}
			}
			if(cm!=null){
				retain(cm);
//				log.debug("InsertHelper conn ,retain:"+retain.get(cm)+",cm:"+cm);
			}
		} catch (Exception e) {
		}
		
		return cm;
	}
	private void setStatus(CollectionManager cm,int v){
			status.put(cm, v);
	}
	private int getStatus(CollectionManager cm){
		return status.get(cm);
}
	private void retain(CollectionManager cm){
		synchronized(retain){
			int v = retain.get(cm);
			retain.put(cm, v+1);
		}
	}
	public boolean readyshutdown(CollectionManager cm){
		//int opos = pos;
		//int npos = (pos+1) % cms.length;
		boolean find = false;
		for(int i=0;i<cms.length;i++){
			if(cms[i]!=cm){
				if(getStatus(cms[i])!=-1){
					find=true;
					pos = i;
					break;
				}
			}
		}
		if(find){
			log.debug("InsertHelper readyshutdown ,retain:"+retain.get(cm)+",cm:"+cm);
			setStatus(cm,-1);
		}
		return find;
	}
	public void shutdown(CollectionManager cm){
		try {
			if(getStatus(cm)==-1){
				log.debug("InsertHelper shutdown ,cm:"+cm);
				cm.disconnectServer();
				setStatus(cm,0);
				return;
			}
		} catch (Exception e) {
		}
	}
	public final void run() {
		long time=1*1000l;//10*60*1000l;
		Map<CollectionManager,Long> lastTime=new HashMap<CollectionManager,Long>();
		for(int i=0;i<cms.length;i++){
			lastTime.put(cms[i], System.currentTimeMillis());
		}
		boolean exec=false;
		long lastExecTime=System.currentTimeMillis();
		while(true){
			sleepping(time);
			if(System.currentTimeMillis()-lastExecTime>=60*1000l){
				exec=true;
				lastExecTime=System.currentTimeMillis();
				////ping
				for(int i=0;i<cms.length;i++){
					if(getStatus(cms[i])==1){
						long ping=cms[i].ping();
						if(ping==-1){
							cms[i].disconnectServer();
							try {
								cms[i].connectServer();	
							} catch (Exception e) {
								setStatus(cms[i],0);
							}
						}else{
							log.debug("ping time:"+ping +"ms");
						}
						
					}
				}
				////
				for(String key:cndMap.keySet()){
					log.debug("集合："+key +"\t 以添加数据量："+cndMap.get(key));
				}
				
				
			}else{
				exec=false;
			}
		
			try {
				action(exec);	
			} catch (Exception e) {
				e.printStackTrace();
			}
			synchronized(retain){
				
				for(int i=0;i<cms.length;i++){
					long post = lastTime.get(cms[i]);
					if(System.currentTimeMillis()-post>10*60*1000l){
						if(getStatus(cms[i])==1){
							if(readyshutdown(cms[i])){
								lastTime.put(cms[i], System.currentTimeMillis());
							}
						}else if(getStatus(cms[i])==0){
							lastTime.put(cms[i], System.currentTimeMillis());
						}
					}
				}
				
				for(int i=0;i<cms.length;i++){
					//if(i==pos)continue;
					if(getStatus(cms[i])==-1){
						long t = lastTime.get(cms[i]);
						int ret = retain.get(cms[i]);
						if(ret<=0){
							if(System.currentTimeMillis()-t>1*60*1000l){
								lastTime.put(cms[i], System.currentTimeMillis());
								retain.put(cms[i],0);
								shutdown(cms[i]);
							}
						} else if(ret>0){
							if(System.currentTimeMillis()-t>20*60*1000l){
								lastTime.put(cms[i], System.currentTimeMillis());
								retain.put(cms[i],0);
								shutdown(cms[i]);
								log.warn("force shutdown."+cms[i]);
							}
						}
					}
				}
			}
			
		}
	}
	private void action(boolean exec) throws Exception{
			for(String key :dataMap.keySet()){
				if(dataMap.get(key)>=execCount || exec){//(exec && dataMap.get(key)>0
					CollectionManager cm=this.conn();
//					List<SolrInputDocument> docs = dataMap.remove(key);
					try {
//						log.debug("action start time:["+new Date() +"]addDocuments size:"+docs.size());
//						cm.addDocuments(docs, key);
//						docCount+=docs.size();
//						log.debug("action end time:["+new Date() +"]addDocuments size:"+docs.size());	
						dataMap.put(key, 0l);
						commit(cm,key);
					} catch (Exception e) {
						log.error("error:",e);
						throw e;
					}finally{
						this.release(cm);
					}
				}
			}
	}
	
	public void sleepping(long time){
		try {
			sleep(time);
		} catch (Exception e) {
			// TODO: handle exception
		}
	}
	public void addDocuments(List<SolrInputDocument> docs, String collectionName,Object uid) throws Exception{
		if(uid ==null ) throw new Exception("get conn failed.");
		CollectionManager cm = (CollectionManager)uid;  
		cm.addDocuments(docs, collectionName);
		docCount+=docs.size();
		if(!dataMap.containsKey(collectionName)){dataMap.put(collectionName, 0l);}
		Long cc=dataMap.get(collectionName);
		cc+=docs.size();
		dataMap.put(collectionName, cc);
		if(!cndMap.containsKey(collectionName)){
			cndMap.put(collectionName, new Long(docs.size()));
		}else{
			cndMap.put(collectionName, cndMap.get(collectionName)+docs.size());
		}
//		if(docCount>=maxCount) commit(cm);
	}
	
//	public  void addDocuments(List<SolrInputDocument> docs, String collectionName) throws Exception{
//		synchronized(dataMap){
//			if(!dataMap.containsKey(collectionName)){
//				dataMap.put(collectionName, new ArrayList<SolrInputDocument>());
//			}
//			List<SolrInputDocument> docsAll =dataMap.get(collectionName);
//			docsAll.addAll(docs);
//			if(docsAll.size()>=maxCount){
//	//			Object uid=this.getUid();
//	//			CollectionManager cm = (CollectionManager)uid;
//				CollectionManager cm=this.conn();
//				log.debug("maxcount start time:["+new Date() +"]addDocuments size:"+docsAll.size());
//				try {
//					cm.addDocuments(docsAll, collectionName);
//					docCount+=docsAll.size();
//					dataMap.remove(collectionName);
//					log.debug("maxcount end time:["+new Date() +"]addDocuments size:"+docsAll.size());
//					commit(cm);	
//				} catch (Exception e) {
//					throw e;
//					// TODO: handle exception
//				}finally{
//					this.release(cm);
//				}
//			}
//		}
//	}
	public void release(Object uid){
		CollectionManager cm = (CollectionManager)uid;
		synchronized(retain){
			int v = retain.get(cm);
			retain.put(cm, v-1);
//			log.debug("release retain:"+retain.get(cm)+",key:"+cm);
		}
	}
	
	public void resetCounter(String collectionName){
		if(cndMap.containsKey(collectionName))
			cndMap.put(collectionName, 0l);
	}
	public void commit(Object uid,String collectionName) throws Exception{
		CollectionManager cm = (CollectionManager)uid;
//		docCount=0;
		
		cm.commitDocuments(collectionName);
		
//		log.debug("commit is exec .exec end time:"+new Date() +" exec sumcount is :"+docCount);
	}
}
