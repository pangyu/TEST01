package com.tensynchina.som.core.config;


import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeoutException;

import javax.servlet.http.HttpServletRequest;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.log4j.BasicConfigurator;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.SolrPingResponse;
import org.apache.solr.cloud.ZkCLI;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.zookeeper.KeeperException;
import org.xml.sax.SAXException;

import com.freeadb.commons.util.Utils;
import com.tensynchina.som.data.CloudBinaryResponseParser;
import com.tensynchina.som.helper.JsonHelper;


public class CollectionManager {
	private static Log log = LogFactory.getLog(CollectionManager.class);
	
	private CloudSolrServer cloudSolrServer=null;
	private static String zkHost = "172.16.0.2:2181,172.16.0.3:2181,172.16.0.4:2181";
	private String solrConfigBasePath = "/home/app/opt/solr-4.4";
	private String solrConfigBaseDir = "multicore";
	private String baseZkSolrConfigDir=ZkController.CONFIGS_ZKNODE;
	private String configNameSuffix="_conf";

	private String defaultCollection = "som";

	private ZkManager zm = null;
	static{
		Properties pp=Utils.getProperties();
		zkHost=pp.getProperty("app.zkHost");
		
	}
	public static void main(String[] args) throws Exception{
		BasicConfigurator.configure();
		/** for test ip
		 * route add 172.16.0.0 mask 255.255.0.0 192.168.1.59 metric 1
		 * .htaccess
		 * 172.16.0.2 test01
		 * 172.16.0.3 test02
		 * 172.16.0.4 test03 
		 * **/
		
		CollectionManager app = new CollectionManager();
		app.connectServer();
		String collectionName="som_new";//"ecshop";
		app.solrConfigBasePath="."+ File.separator+"solrconfig";
		//out("Collections: "+app.getCollections());
		//out("liveNodes: "+app.getLiveNodes());
		
		String solrLocalConfigPath=app.solrConfigBasePath + File.separator + app.solrConfigBaseDir + File.separator
		+ collectionName + File.separator + "conf";
		
		/** -- BEGIN 创建集合 -- ** /
		out("uploadconfig: "+app.uploadConfig(solrLocalConfigPath,collectionName));
		out("createCollection: "+app.createCollection(collectionName));
		//** -- END 创建集合 -- */
		
		/** -- BEGIN 更新集合 -- ** /
		//out("downloadConfig: "+app.downloadConfig(solrLocalConfigPath, collectionName));
		// -- 修改配置内容 --
		out("uploadconfig: "+app.uploadConfig(solrLocalConfigPath,collectionName));
		out("updateCollection: "+app.updateCollection(collectionName));
		//** -- END 更新集合 -- **/

		/** -- BEGIN 分割Shard -- * /
		//out("splitShard: "+app.splitShard(collectionName, "shard2_1"));
		out("deleteShard: "+app.deleteShard(collectionName, "shard2_1"));
		//** -- END 分割Shard -- */
		
		/** -- BEGIN 删除集合 -- * /
		out("deleteCollection: "+app.deleteCollection(collectionName));
		out("clearConfig :"+app.clearConfig(collectionName));
		//** -- END 删除集合 -- */

		/** -- BEGIN 创建副本 -- * /
		out("createReplica: "+app.createReplica("ecshop_shard1_cp04", collectionName, "shard1", "http://172.16.0.2:8983/solr"));
		//** -- END 创建副本  -- */
		/** -- BEGIN 查看状态 -- */
//		HashMap<String,String> liveNodes=app.getLiveNodes();
//		out("getLiveNodes: "+liveNodes);
//		for(String key:liveNodes.keySet()){
//			app.getCoreStatus(liveNodes.get(key));
//		}
		Set<String> collections=app.getCollections();
		log.debug("getCollections: "+app.getCollections());
//		for(String name:collections){
//			out("getShards("+name+"): "+app.getShards(name));
//		}
//		HashMap<String, HashMap<String, Object>> cstatus = app.getCoreStatus();
//		out("getCoreStatus :"+cstatus);
		Map<String, Map<String, Object>> status = app.getCollectionStatus(collections);
		log.debug("getCollectionStatus:"+status);
		//app.getCoreStatus("");
		//** -- END 查看状态 -- */

		/** -- BEGIN 插入数据 -- */ 
		//collectionName="tb";
		ArrayList<SolrInputDocument> docs=new ArrayList<SolrInputDocument>();
		int commitWithinMs=10;
//		for(int i=0;i<1;i++){
//			SolrInputDocument doc=new SolrInputDocument();
//			doc.addField("id", "500600"+i);
//			doc.addField("subject", "再试一次 100"+i);
//			doc.addField("url", "http://www.baidu.com/q=600"+i);
//			doc.addField(
//					"content",
//					"500600"+i+" 11月3日以来，截至9月14日，QDII基金全体实现正收益，平均涨幅达到6.21%。其中，涨幅最高的是上投全球天然资源股票，为15.34%；其次是华宝油气、富国全球顶级消费品股票，分别上涨14.59%和12.31%。还有招商全球资源股票、华安标普石油指数、交银全球资源股票、汇添富黄金及贵金属等涨幅都在10%以上。易方达黄金主题，诺安全球黄金的涨幅也很可观");
//			
//			docs.add(doc);
//			if(i%500==0){
//				app.addDocuments(docs, collectionName);//(docs, commitWithinMs, collectionName);
//				System.out.println("adddocuments: "+i);
//				docs.clear();
//			}
//		}
//		if(docs.size()>0){
//			app.addDocuments(docs, collectionName);//(docs, commitWithinMs, collectionName);
//		}
		app.commitDocuments(collectionName);
		//** -- END 插入数据 -- */
		

		/** -- begin 上传配置文件 --* /
		app.testUploadCfg("solrconfig/multicore/tb/conf", "som");
		//*/
		
		app.disconnectServer();
	}
//	static void out(String info) {
//		System.out.println(new SimpleDateFormat("yyyyMMdd hhmmss SSS ").format(new Date())+info);
//	}
	/**
	 * 
	 */
	public CollectionManager(){

	}
	/**
	 * 连接到服务器
	 * @return
	 * @throws MalformedURLException
	 */
	public int connectServer() throws MalformedURLException{
		final int zkClientTimeout = 20000;
		final int zkConnectTimeout = 1000;
		try {
			cloudSolrServer=new CloudSolrServer(zkHost);
		} catch (MalformedURLException e) {
			log.error("new CloudSolrServer("+zkHost+") failed!");
			throw e;
		}
		log.debug("The Cloud SolrServer Instance has benn created!");

		cloudSolrServer.setDefaultCollection(defaultCollection);
		cloudSolrServer.setZkClientTimeout(zkClientTimeout);
		cloudSolrServer.setZkConnectTimeout(zkConnectTimeout);
		
		zm = new ZkManager(cloudSolrServer);
		
		cloudSolrServer.connect();
		log.debug("The cloud Server has been connected !!!!");
		return 0;
	}
	
	public void commitDocuments(String collectionName) throws SolrServerException, IOException {
		try {
			cloudSolrServer.setDefaultCollection(collectionName);
			cloudSolrServer.commit();
		} catch (SolrServerException e) {
			log.error("SolrServerException in commitDocument",e);
			throw e;
		} catch (IOException e) {
			log.error("IOException in commitDocument",e);
			throw e;
		}
	}
	/**
	 * 
	 * @return
	 */
	public long ping(){
		long p = -1;
		try {
			SolrPingResponse spr = cloudSolrServer.ping();
			p=spr.getElapsedTime();
		} catch (Exception e) {
			log.error("ping failed !!!", e);
		}
		return p;
	}
	/**
	 * 查询数据
	 * @param request
	 * @return
	 * @throws Exception
	 */
	public String query(HttpServletRequest request,String collectionName)throws Exception{
		SolrQuery sq = new SolrQuery();
		Map<String,String[]> mm=request.getParameterMap();
		for(String key:mm.keySet()){
			sq.setParam(key, request.getParameter(key));
		}
		cloudSolrServer.setDefaultCollection(collectionName);
		QueryRequest solrrequest = new QueryRequest(sq);
			//String wt = solrrequest.getParams().get("wt");
			//String path = solrrequest.getPath();

		solrrequest.setResponseParser(new CloudBinaryResponseParser(solrrequest));
		QueryResponse response = solrrequest.process( cloudSolrServer );//solrServer.queryAndStreamResponse(query, new CloudStreamingResponseCallback());
		Object is = response.getResponse().get("Stream");
		String encode = (String)response.getResponse().get("encode");
//			System.out.println("encode:"+encode);
//			System.out.println("stream:"+new String((byte[])is,encode));
		
		return new String((byte[])is,encode);
	}
	/**
	 * 断开到服务器的连接
	 * @return
	 */
	public int disconnectServer(){
		cloudSolrServer.shutdown();
		log.debug("The cloud Server has been disconnected !!!!");
		return 0;
	}
	/**
	 * 设置默认的集合名
	 * @param collectionName
	 */
	public void setDefaultCollection(String collectionName){
		defaultCollection=collectionName;
		if(cloudSolrServer!=null){
			cloudSolrServer.setDefaultCollection(collectionName);
		}
	}
	/**
	 * 添加文档到索引
	 * @param docs			文档列表
	 * @param collectionName  要插入的集合名
	 * @return
	 * @throws IOException 
	 * @throws SolrServerException 
	 */
	public int addDocuments(Collection<SolrInputDocument> docs,String collectionName) throws SolrServerException, IOException{//,int commitWithinMs,String collectionName){
		if(cloudSolrServer==null){
			log.error("cloudSolrServer is null!");
			throw new NullPointerException("cloudSolrServer is null!");
		}
		if(!defaultCollection.equals(collectionName)){
			cloudSolrServer.setDefaultCollection(collectionName);
		}
		cloudSolrServer.add(docs);//, commitWithinMs);
//		if(!defaultCollection.equals(collectionName)){
//			cloudSolrServer.setDefaultCollection(defaultCollection);
//		}
		return 0;
	}
	/**
	 * 清空集合数据
	 * @param collectionName
	 * @return
	 * @throws SolrServerException
	 * @throws IOException
	 */
	public int deleteAllDocuments(String collectionName) throws SolrServerException, IOException{//,int commitWithinMs,String collectionName){
		if(cloudSolrServer==null){
			log.error("cloudSolrServer is null!");
			throw new NullPointerException("cloudSolrServer is null!");
		}
		cloudSolrServer.setDefaultCollection(collectionName);
		cloudSolrServer.deleteByQuery("*:*");//, commitWithinMs);
		cloudSolrServer.commit();
//		if(!defaultCollection.equals(collectionName)){
//			cloudSolrServer.setDefaultCollection(defaultCollection);
//		}
		return 0;
	}
	/**
	 * 清除Zookeeper上的指定集合的配置信息
	 * @param collectionName 集合名
	 * @return
	 * @throws InterruptedException 线程阻断
	 * @throws TimeoutException 访问超时
	 * @throws IOException 数据传输异常
	 * @throws ParserConfigurationException 配置解析失败 
	 * @throws SAXException XML解析失败
	 * @throws KeeperException Zookeeper操作异常
	 */
	public void clearConfig(String collectionName) 
		throws InterruptedException, TimeoutException, IOException, 
			ParserConfigurationException, SAXException, KeeperException{


			String[] zkparams = new String[] {
					"-cmd",
					"clear",
					"-zkhost",
					zkHost,
					baseZkSolrConfigDir+"/"+collectionName + configNameSuffix };
			try {
				ZkCLI.main(zkparams);
			} catch (InterruptedException e) {
				log.error("InterruptedException, zk clear conf failed !!!", e);
				throw e;
			} catch (TimeoutException e) {
				log.error("TimeoutException, zk clear conf failed !!!", e);
				throw e;
			} catch (IOException e) {
				log.error("zk clear conf failed !!!", e);
				throw e;
			} catch (ParserConfigurationException e) {
				log.error("zk clear conf failed !!!", e);
				throw e;
			} catch (SAXException e) {
				log.error("zk clear conf failed !!!", e);
				throw e;
			} catch (KeeperException e) {
				log.error("zk clear conf failed !!!", e);
				throw e;
			}
	}
	/**
	 * 下载Zookeeper上的指定集合的配置信息
	 * @param collectionName 集合名
	 * @param configPath 保存配置文件的位置
	 * @throws InterruptedException 线程阻断
	 * @throws TimeoutException 访问超时
	 * @throws IOException 数据传输异常
	 * @throws ParserConfigurationException 配置解析失败 
	 * @throws SAXException XML解析失败
	 * @throws KeeperException Zookeeper操作异常
	 */
	public void downloadConfig(String configPath,String collectionName) 
		throws InterruptedException, TimeoutException, IOException, ParserConfigurationException, 
			SAXException, KeeperException{
		try {

			String[] zkparams = new String[] {
					"-cmd",
					"downconfig",
					"-zkhost",
					zkHost,
					"-confdir",	configPath,
					"-confname", collectionName + configNameSuffix };
			ZkCLI.main(zkparams);
			
		} catch (InterruptedException e) {
			log.error("InterruptedException, zk clear conf failed !!!", e);
			throw e;
		} catch (TimeoutException e) {
			log.error("TimeoutException, zk clear conf failed !!!", e);
			throw e;
		} catch (IOException e) {
			log.error("zk clear conf failed !!!", e);
			throw e;
		} catch (ParserConfigurationException e) {
			log.error("zk clear conf failed !!!", e);
			throw e;
		} catch (SAXException e) {
			log.error("zk clear conf failed !!!", e);
			throw e;
		} catch (KeeperException e) {
			log.error("zk clear conf failed !!!", e);
			throw e;
		}
	}
	/**
	 * 将configPath中指定的config目录下的所有文件上传到zookeeper中的collectionName对应的目录中
	 * @param configPath
	 * @param collectionName
	 * @return
	 */
	public void uploadConfig(String configPath,String collectionName)
		throws InterruptedException, TimeoutException, IOException, ParserConfigurationException, 
			SAXException, KeeperException{
		if(!new File(configPath).exists()){
			log.error("local config path ["+configPath+"] not exists!");
			throw new IOException("config path ["+configPath+"] not exists");
		}
		
		// 上传solr config到zookeeper,并设置别名

		String[] zkparams = new String[] {
				"-cmd",
				"upconfig",
				"-zkhost",
				zkHost,
				"-confdir",	configPath,
				"-confname", collectionName + configNameSuffix };
		ZkCLI.main(zkparams);
	}
	/**
	 * 分割数据段落
	 * @param collection 集合名
	 * @param shard 段落名
	 * http://172.16.0.4:8983/solr/admin/collections?action=SPLITSHARD&collection=som&shard=shard4
	 * @throws IOException 
	 * @throws SolrServerException 
	 */
	public int splitShard(String collection,String shard) throws SolrServerException, IOException{
		ModifiableSolrParams params = new ModifiableSolrParams();
		SolrRequest request = new QueryRequest(params);
		request.setPath("/admin/collections");
		// new core
		params.set("action",CollectionParams.CollectionAction.SPLITSHARD.toString());
		params.set("collection", collection);
		params.set("shard", shard);
		requestCloud(request);
		return 0;
	}
	/**
	 * 删除数据段落  只能删除被分隔之后的段落
	 * @param collection 集合名
	 * @param shard 段落名
	 * @return
	 * @throws IOException 
	 * @throws SolrServerException 
	 */
	public int deleteShard(String collection,String shard) throws SolrServerException, IOException{
		ModifiableSolrParams params = new ModifiableSolrParams();
		SolrRequest request = new QueryRequest(params);
		request.setPath("/admin/collections");
		// new core
		params.set("action",CollectionParams.CollectionAction.DELETESHARD.toString());
		params.set("collection", collection);
		params.set("shard", shard);
		requestCloud(request);
		return 0;
		
	}
	
	/**
	 * 获取段落列表
	 * @param collectionName
	 * @return {shardName={propName=propValue,replicas={}}}
	 */
	public Map<String,Map<String,Object>> getShards(String collectionName){
		Map<String, Map<String,Object>> maps = new HashMap<String, Map<String,Object>>();
		ClusterState clusterState = cloudSolrServer.getZkStateReader()
				.getClusterState();
		Map<String, Slice> slices = clusterState.getSlicesMap(collectionName);
		
		if (slices == null)
			return maps;
		
		
		for (Map.Entry<String, Slice> entry : slices.entrySet()) {
			
			Slice shard=entry.getValue();
			String shardName = shard.getName();
			//out("shard "+shardName+" "+shard.getProperties());
			maps.put(shardName,entry.getValue().getProperties());
		}
		return maps;
	}
	/**
	 * 获取核心列表
	 * @param collectionName
	 * @return {coreName={propName=propValue}}
	 */
	public Map<String, Map<String,String>> getCores(String collectionName){
		Map<String, Map<String,String>> maps = new HashMap<String, Map<String,String>>();
		ClusterState clusterState = cloudSolrServer.getZkStateReader()
				.getClusterState();
		Map<String, Slice> slices = clusterState.getSlicesMap(collectionName);
		
		if (slices == null)
			return maps;
		
		
		for (Map.Entry<String, Slice> entry : slices.entrySet()) {
			
			Slice shard=entry.getValue();
			String shardName = shard.getName();
			//out("shard "+shardName+" "+shard.getProperties());
			Map<String, Replica> cores = shard.getReplicasMap();
			for (Map.Entry<String, Replica> coreEntry : cores.entrySet()) {
				Replica core=coreEntry.getValue();
				HashMap<String,String> coreProps= new HashMap<String, String>();
				coreProps.put("shard_name", shardName);
				coreProps.put("collection_name", collectionName);
				coreProps.put("state", core.get(ZkStateReader.STATE_PROP)+"");
				coreProps.put("core_name", core.get(ZkStateReader.CORE_NAME_PROP)+"");
				coreProps.put("node_name", core.get(ZkStateReader.NODE_NAME_PROP)+"");
				coreProps.put("base_url", core.get(ZkStateReader.BASE_URL_PROP)+"");
				
				maps.put(coreProps.get("core_name"),coreProps);
			}
		}
		return maps;
	}
	/**
	 * 创建核心副本
	 * @param replicaName
	 * @param collectionName
	 * @param shardName
	 * @param solrBasePath
	 * @return
	 * @throws SolrServerException 
	 * @throws IOException 
	 */
	public int createReplica(String replicaName,String collectionName,String shardName,String solrBasePath) 
		throws SolrServerException, IOException{
		ModifiableSolrParams coreparams = new ModifiableSolrParams();
		SolrRequest corerequest = new QueryRequest(coreparams);
		// new core
		coreparams.set("action",
				CoreAdminParams.CoreAdminAction.CREATE.toString());
		coreparams.set("name", replicaName);
		coreparams.set("collection", collectionName);
		coreparams.set("collection.configName", collectionName + "_conf");
		corerequest.setPath("/admin/cores");
		HttpSolrServer coreServer = new HttpSolrServer(solrBasePath);
		coreparams.set("shard", shardName);
		try {
			coreServer.request(corerequest);
		} catch (SolrServerException e) {
			log.error("SolrServerException in createReplica");
			throw e;
		} catch (IOException e) {
			log.error("IOException in createReplica");
			throw e;
		}
		return 0;
	}
	/**
	 * 删除核心副本
	 * @param replicaName 核心名
	 * @param solrBasePath 所处节点地址
	 * @return
	 */
	public void deleteReplica(String replicaName,String solrBasePath)
		throws SolrServerException, IOException{
		//http://172.16.0.2:8983/solr/admin/cores?action=unload&core=som_core_x05c
		ModifiableSolrParams coreparams = new ModifiableSolrParams();
		SolrRequest corerequest = new QueryRequest(coreparams);
		// new core
		coreparams.set("action",
				CoreAdminParams.CoreAdminAction.UNLOAD.toString());
		coreparams.set("core", replicaName);
		corerequest.setPath("/admin/cores");
		HttpSolrServer coreServer = new HttpSolrServer(solrBasePath);
		try {
			coreServer.request(corerequest);
		} catch (SolrServerException e) {
			log.error("SolrServerException in deleteReplica");
			throw e;
		} catch (IOException e) {
			log.error("IOException in deleteReplica");
			throw e;
		}
	}
	/**
	 * 获取集合列表
	 * @return
	 */
	public Set<String> getCollections(){
		ClusterState cs = cloudSolrServer.getZkStateReader().getClusterState();

		return cs.getCollections();
	}
	/**
	 * 发送请求到SolrCloud
	 * @param request
	 * @return
	 * @throws SolrServerException 
	 * @throws IOException 
	 */
	public NamedList<Object> requestCloud(SolrRequest request) throws SolrServerException, IOException{	
		if(cloudSolrServer==null){
			log.error("cloudSolrServer is null!");
			throw new NullPointerException("cloudSolrServer is null!");
		}
		try {
			//cloudServer= new CloudSolrServer(zkHost);
			NamedList<Object> r = cloudSolrServer.request(request);
			log.debug("request [" + request.getPath() + "]  on cloud success.");
			return r;
		}  catch (SolrServerException e) {
			log.error("SolrServerException in requestCloud");
			throw e;
		} catch (IOException e) {
			log.error("IOException in requestCloud");
			throw e;
		}
	}
	
	/**
	public int tryRequestSolr(SolrRequest request){
		//HttpSolrServer httpServer=new HttpSolrServer("http://192.168.1.132:8983/solr/collection1/");
		
		HashMap<String,String> liveNodes=getLiveNodes();
		HttpSolrServer coreServer = null;
		for (String node : liveNodes.keySet()){
			String basepath = liveNodes.get(node);
			try {
				coreServer = new HttpSolrServer(basepath);
				coreServer.setConnectionTimeout(15000);
				coreServer
						.setSoTimeout((int) (CollectionsHandler.DEFAULT_ZK_TIMEOUT * 5));
				coreServer.request(request);
				out("request [" + request.getPath() + "]  on ["+node+"] success.");
				break;
			} catch (Exception e) {
				e.printStackTrace();
				out("request [" + request.getPath() + "]  on ["+node+"] failed! "
						+ e.getMessage());
			} finally {
				if (coreServer != null) {
					coreServer.shutdown();
					coreServer=null;
				}
			}
		}
		return 0;
	}
	*/
	/**
	 * 获取当前存活的节点列表
	 */
	public HashMap<String,String> getLiveNodes(){
		HashMap<String,String> liveNodes=new HashMap<String, String>();

		ClusterState cs = cloudSolrServer.getZkStateReader().getClusterState();
		SolrZkClient zkClient = null;
		try {
			zkClient = new SolrZkClient(zkHost, 10000);
			Set<String> nodeNames = cs.getLiveNodes();
			for (String nn : nodeNames) {
				//System.out.println("nodeName:" + nn);
				String basepath = zkClient.getBaseUrlForNodeName(nn);
				//System.out.println(nn + "->urlpath:" + basepath);
				liveNodes.put(nn, basepath);
			}
		} finally {

			if (zkClient != null) {
				zkClient.close();
			}

		}
		return liveNodes;
	}
	/**
	 * 判断集合是否存在
	 * @param collectionName
	 * @return
	 */
	public boolean isCollectionExist(String collectionName){
		Set<String> collections = getCollections();
		if(collections!=null&&collections.contains(collectionName)){
			return true;
		}
		return false;
	}
	/**
	 * 删除集合
	 * @param collectionName
	 * @return
	 * @throws SolrServerException
	 * @throws IOException
	 */
	public int deleteCollection(String collectionName) throws SolrServerException, IOException{
		
		// 检查集合是否已存在
		if(!isCollectionExist(collectionName)){
			log.warn("Collection ["+collectionName+"] is not exist.");
			return -201;
		}
		
		ModifiableSolrParams coreparams = new ModifiableSolrParams();
		coreparams.set("action",
				CollectionParams.CollectionAction.DELETE.toString());
		coreparams.set("name", collectionName);
		
		SolrRequest request = new QueryRequest(coreparams);
		request.setPath("/admin/collections"); // 请求位置
		
		requestCloud(request);
		return 0;
	}

	public int createCollection(String collectionName,int rf,int sn,int msp,String nodes) throws Exception{
		if(collectionName==null||collectionName.isEmpty()){
			log.warn("collectionName not set!");
			return -101;
		}
		if(cloudSolrServer==null){
			log.warn("cloudSolrServer not connected!");
			return -102;
		}

		
		
		// 检查集合是否已存在
		if(isCollectionExist(collectionName)){
			log.warn("Collection ["+collectionName+"] is exist.");
			return -201;
		}
		
		// 创建集合
		int numShards = 3;
		int maxShards = 3;
		int replicationFactor = 3;
		numShards=sn;
		maxShards=msp;
		replicationFactor=rf;
		ModifiableSolrParams coreparams = new ModifiableSolrParams();
		coreparams.set("action",
				CollectionParams.CollectionAction.CREATE.toString());
		coreparams.set("name", collectionName);
		coreparams.set("replicationFactor", replicationFactor);
		coreparams.set("numShards", numShards);
		coreparams.set("maxShardsPerNode", maxShards);
		coreparams.set("collection.configName", collectionName+ "_conf"); // 关联配置信息
		// coreparams.set("createNodeSet","192.168.1.15:8983_solr,192.168.1.16:8983_solr");//not
		if(!Utils.isEmpty(nodes))
		 coreparams.set("createNodeSet",nodes);
		// defined ,default
		// all node.
		
		SolrRequest request = new QueryRequest(coreparams);
		request.setPath("/admin/collections"); // 请求位置
		
		requestCloud(request);
		return 0;
	}
	/**
	 * 创建集合
	 * @param collectionName 集合名
	 * @return
	 * @throws SolrServerException
	 * @throws IOException
	 */
	public int createCollection(String collectionName) throws SolrServerException, IOException{
		if(collectionName==null||collectionName.isEmpty()){
			log.warn("collectionName not set!");
			return -101;
		}
		if(cloudSolrServer==null){
			log.warn("cloudSolrServer not connected!");
			return -102;
		}

		
		
		// 检查集合是否已存在
		if(isCollectionExist(collectionName)){
			log.warn("Collection ["+collectionName+"] is exist.");
			return -201;
		}
		
		// 创建集合
		int numShards = 3;
		int maxShards = 3;
		int replicationFactor = 3;
		ModifiableSolrParams coreparams = new ModifiableSolrParams();
		coreparams.set("action",
				CollectionParams.CollectionAction.CREATE.toString());
		coreparams.set("name", collectionName);
		coreparams.set("replicationFactor", replicationFactor);
		coreparams.set("numShards", numShards);
		coreparams.set("maxShardsPerNode", maxShards);
		coreparams.set("collection.configName", collectionName+ "_conf"); // 关联配置信息
		// coreparams.set("createNodeSet","192.168.1.15:8983_solr,192.168.1.16:8983_solr");//not
		// defined ,default
		// all node.
		
		SolrRequest request = new QueryRequest(coreparams);
		request.setPath("/admin/collections"); // 请求位置
		
		requestCloud(request);
		return 0;
	}
	/**
	 * 更新集合配置 （启用zookeeper中对应的配置信息的变化）
	 * @param collectionName
	 * @return
	 * @throws SolrServerException
	 * @throws IOException
	 */
	public int updateCollection(String collectionName) throws SolrServerException, IOException{
		
		// 检查集合是否已存在
		if(!isCollectionExist(collectionName)){
			log.warn("Collection ["+collectionName+"] is not exist.");
			return -201;
		}

		
		ModifiableSolrParams coreparams = new ModifiableSolrParams();
		coreparams.set("action",
				CollectionParams.CollectionAction.RELOAD.toString());
		coreparams.set("name", collectionName);
		
		SolrRequest request = new QueryRequest(coreparams);
		request.setPath("/admin/collections"); // 请求位置
		
		requestCloud(request);
		return 0;
	}
	/**
	 * 获取指定节点的所有核心状态
	 * @param livenode
	 * @return
	 * @throws IOException 
	 */	
	public HashMap<String,HashMap<String,Object>> getCoreStatus(String livenode) throws Exception{
		//http://172.16.0.4:8983/solr/admin/cores?action=status&wt=json
		String url=livenode+"/admin/cores?action=status&wt=json";
		String content=requestUrl(url);
//		JSONObject jobj=new JSONObject(content);
		Map<String,Object> mm=JsonHelper.dealJsonObject(content);
//		JSONObject status=(JSONObject)jobj.get("status");

		Map<String,Object> rs=(Map<String ,Object>)mm.get("responseHeader");
		if(!rs.get("status").toString().equals("0")){
			log.error("request failed ! "+url+" \n "+mm);
			return null;
		}
		Map<String,Object> status=(Map<String ,Object>)mm.get("status");
		HashMap<String,HashMap<String,Object>> cores=new HashMap<String, HashMap<String,Object>>();
		for(Object key:status.keySet()){
			String coreName=key.toString();
//			JSONObject s=(JSONObject)((JSONObject)status.get((String)coreName)).get("index");
			Map<String ,Object> ts=(Map<String,Object>)status.get(coreName);
			Map<String ,Object> s=(Map<String,Object>)ts.get("index");
			HashMap<String,Object> c=new HashMap<String, Object>();
			c.put("numDocs", s.get("numDocs"));
			c.put("maxDoc", s.get("maxDoc"));
			c.put("sizeInBytes", s.get("sizeInBytes"));
			c.put("size", s.get("size"));
			c.put("deletedDocs", s.get("deletedDocs"));
			c.put("segmentCount", s.get("segmentCount"));
			cores.put(coreName,c);
		}
		//out(cores.toString());
		return cores;
	}
	/**
	 * 获取所有存活的节点的核心状态列表
	 * @return
	 * @throws IOException 
	 */
	public HashMap<String,HashMap<String,Object>> getCoreStatus() throws Exception{
		HashMap<String, HashMap<String, Object>> cstatus= new HashMap<String, HashMap<String,Object>>();
		HashMap<String,String> liveNodes=getLiveNodes();
		for(String key:liveNodes.keySet()){
			HashMap<String, HashMap<String, Object>> cs = getCoreStatus(liveNodes.get(key));
			if(cs!=null){
				
				cstatus.putAll(cs);
			}else{
				log.error("getCoreStatus("+liveNodes.get(key)+") failed!");
			}
		}
		return cstatus;
	}
	/**
	 * 获取指定集合的状态
	 * @param collections
	 * @return
	 * getCollectionStatus:{
	 * 	tb={ // collection
	 * 		shard1={ // shard
	 * 			range=80000000-d554ffff, 
	 * 			state=active,
	 * 			replicas={
	 * 				tb_shard1_replica1={ // replica (core)
	 * 					state=active, 
	 * 					collection=tb, 
	 * 					base_url=http://172.16.0.4:8983/solr, 
	 * 					size=65 bytes, 
	 * 					segmentCount=0, 
	 * 					shard=shard1, 
	 * 					numDocs=0, 
	 * 					leader=null, 
	 * 					name=core_node8, 
	 * 					maxDoc=0, 
	 * 					core=tb_shard1_replica1, 
	 * 					sizeInBytes=65, 
	 * 					node_name=172.16.0.4:8983_solr, 
	 * 					deletedDocs=0
	 * 				},
	 *			},
	 * @throws IOException 
	 */
	@SuppressWarnings("unchecked")
	public Map<String,Map<String,Object>> getCollectionStatus(Set<String> collections) throws Exception{
		Map<String, Map<String, Object>> cstatus= new HashMap<String, Map<String,Object>>();
		HashMap<String, HashMap<String, Object>> status = getCoreStatus();
		//out("coreStatus : "+status);
		// {collectionName=>{size=>"51M"},}
		for(String cn:collections){
			Map<String, Map<String, Object>> shards = getShards(cn);
			Map<String, Object> ccs = new HashMap<String, Object>();
			for(String sn:shards.keySet()){
				Map<String, Object> shard = (Map<String,Object>)shards.get(sn);
				Map<String,Object> replicas = (Map<String,Object>)shard.get("replicas");
				Map<String, Object> st = new HashMap<String, Object>();
				st.put("range", 		shard.get("range"));
				st.put("state", 		shard.get("state"));
				st.get("state");
				
				//out("shards.get("+sn+"):"+replicas);
				Map<String, Object> ss = new HashMap<String, Object>();
				for(String rn:replicas.keySet()){
					//out("replicas.get("+rn+"):"+replicas.get(rn));//org.apache.solr.common.cloud.Replica
					//core_node2=core_node2:{"state":"active","core":"ecshop_shard1_replica3","node_name":"172.16.0.3:8983_solr","base_url":"http://172.16.0.3:8983/solr","leader":"true"
					Replica r = (org.apache.solr.common.cloud.Replica)replicas.get(rn);
					String core=(String)r.get("core");
					Map<String, Object> cs = new HashMap<String, Object>();
					cs.put("state", 		r.get("state"));
					cs.put("node_name", 	r.get("node_name"));
					cs.put("base_url", 		r.get("base_url"));
					cs.put("leader", 		r.get("leader"));

					cs.put("core", 			core);
					cs.put("shard", 		sn);
					cs.put("name", 			rn);
					cs.put("collection",	cn);
					
					if(status.containsKey(core)){
						HashMap<String, Object> s = status.get(core);
						// segmentCount=0, numDocs=0, maxDoc=0, sizeInBytes=65, deletedDocs=0, size=65 bytes}
						cs.put("segmentCount", 	s.get("segmentCount"));
						cs.put("numDocs", 		s.get("numDocs"));
						cs.put("maxDoc", 		s.get("maxDoc"));
						cs.put("sizeInBytes", 	s.get("sizeInBytes"));
						cs.put("deletedDocs", 	s.get("deletedDocs"));
						cs.put("size", 			s.get("size"));
					}
					
					ss.put(core,cs);
				}
				st.put("replicas", 		ss);
				ccs.put(sn,st);
			}
			cstatus.put(cn, ccs);
			//out("Collections "+cn+" shards : "+ccs);
		}
		return cstatus;
	}
//	
//	public void testUploadCfg(String configPath,String collectionName){
//		File dir = new File(configPath);
//		if(dir.exists()){
//			File[] fs = dir.listFiles();
//			for(File f:fs){
//				if(!f.isDirectory()&&!f.isHidden()){
//					try {
//						uploadConfig(collectionName,f.getName(),FileUtils.readFileToByteArray(f));
//					} catch (IOException e) {
//						e.printStackTrace();
//					}
//				}
//			}
//		}
//	}
	
	/**
	 * 上传配置文件到zookeeper中
	 * @param collectionName 集合名
	 * @param fileName 文件名
	 * @param datas 文件内容
	 * @return
	 * @throws Exception 
	 */
	public int uploadConfig(String collectionName,String fileName,byte[] datas) throws Exception{
		try {
			zm.uploadToCfgPath(collectionName+"_conf", fileName, datas);
		} catch (Exception e) {
			log.error("Exception in uploadConfig ",e);
			throw e;
		}
		return 0;
	}
	
	/**
	 * 发送http请求
	 * @param url
	 * @return
	 * @throws IOException 
	 */
	protected String requestUrl(String url) throws IOException {
		HttpClient hc=new DefaultHttpClient();
		HttpGet get=new HttpGet(url);
		try {
			HttpResponse resp=hc.execute(get);
			HttpEntity entity = resp.getEntity();
			if(entity!=null){
				String content=convertStreamToString(entity.getContent());
				return content;
			}
		} catch (ClientProtocolException e) {
			log.error("ClientProtocolException in requestUrl "+url,e);
			throw e;
		} catch (IOException e) {
			log.error("IOException in requestUrl "+url,e);
			throw e;
		} finally{
			get.abort();
		}
		return null;
	}
	/**
	 * 读取流内的文本
	 * @param is
	 * @return
	 */
	protected String convertStreamToString(InputStream is) {      
        BufferedReader reader = new BufferedReader(new InputStreamReader(is));      
        StringBuilder sb = new StringBuilder();      
       
        String line = null;      
        try {
            while ((line = reader.readLine()) != null) {  
                sb.append(line + "\n");      
            }
        } catch (IOException e) {      
            e.printStackTrace();
        } finally {
            try {      
                is.close();     
            } catch (IOException e) {
               e.printStackTrace();      
            }
        }  
        return sb.toString();
    }  

}
/***
 * 查看节点状态
 * http://172.16.0.4:8983/solr/admin/cores?action=status&wt=json
 * http://172.16.0.4:8983/solr/admin/cores?action=status&core=som_shard1_replica1
 * http://172.16.0.2:8983/solr/zookeeper?detail=true&path=%2Fclusterstate.json
 * 
 * 创建一个核心
 * http://172.16.0.5:8983/solr/admin/cores?action=create&name=som_core_x03&collection=som&collection.configName=som_conf&shard=shard4
 * 改名
 * http://172.16.0.2:8983/solr/admin/cores?action=rename&core=som_core_x05&other=som_core_x05c
 * 重新加载
 * http://172.16.0.2:8983/solr/admin/cores?action=reload&core=som_core_x05c
 * 卸载
 * http://172.16.0.2:8983/solr/admin/cores?action=unload&core=som_core_x05c
 * 卸载后报了一个加载不到的错误
 * 
 * http://172.16.0.3:8983/solr/admin/cores?action=STATUS
 * http://localhost:8983/solr/admin/cores?action=STATUS&core=core0
 * 
 * http://192.168.1.16:8983/solr/tb_shard2_replica1/replication?command=details&wt=json
 * route add 172.16.0.0 mask 255.255.0.0 192.168.1.59 metric 1
 * 
 * 
 * $ ./zkCli.sh -server 172.16.0.2:2181,172.16.0.3:2181,172.16.0.4:2181 ls /
 * **/