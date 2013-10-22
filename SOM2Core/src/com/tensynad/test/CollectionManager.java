package com.tensynad.test;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.request.QueryRequest;
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

public class CollectionManager {
private static Log log = LogFactory.getLog(CollectionManager.class);
	
	private CloudSolrServer cloudSolrServer=null;
	private String zkHost = "172.16.0.2:2181,172.16.0.3:2181,172.16.0.4:2181";
	private String solrConfigBasePath = "/home/app/opt/solr-4.4";
	private String solrConfigBaseDir = "multicore";
	private String baseZkSolrConfigDir=ZkController.CONFIGS_ZKNODE;
	private String configNameSuffix="_conf";
	public static void main(String[] args){
		/** for test ip
		 * route add 172.16.0.0 mask 255.255.0.0 192.168.1.59 metric 1
		 * .htaccess
		 * 172.16.0.2 test01
		 * 172.16.0.3 test02
		 * 172.16.0.4 test03 
		 * **/
		
		CollectionManager app = new CollectionManager();
		app.connectServer();
		String collectionName="som";
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

		/** -- BEGIN 删除集合 -- * /
		out("createReplica: "+app.createReplica("som_shard1_cp04", collectionName, "shard1", "http://172.16.0.4:8983/solr"));
		//** -- END 删除集合 -- */
		/** -- BEGIN 查看状态 -- */
		out("getLiveNodes: "+app.getLiveNodes());
		Set<String> collections=app.getCollections();
		out("getCollections: "+app.getCollections());
		for(String name:collections){
			out("getShards("+name+"): "+app.getShards(name));
			Map<String,Map<String,Object>> shardMap=app.getShards(name);
			for(String key:shardMap.keySet()){
				out("KKKKKKKKKKKKKKKK:"+key);
				for(String  k:shardMap.get(key).keySet()){
					out("key:"+k+" \t value:"+shardMap.get(key).get(k));
				}
			}
			break;
		}
		//** -- END 查看状态 -- */

		/** -- BEGIN 插入数据 -- * /
		//collectionName="tb";
		ArrayList<SolrInputDocument> docs=new ArrayList<SolrInputDocument>();
		int commitWithinMs=10;
		for(int i=0;i<999999;i++){
			SolrInputDocument doc=new SolrInputDocument();
			doc.addField("id", "200"+i);
			doc.addField("title", "再试一次 200"+i);
			doc.addField("url", "http://www.baidu.com/q=200"+i);
			doc.addField(
					"content",
					"200"+i+" 11月3日以来，截至9月14日，QDII基金全体实现正收益，平均涨幅达到6.21%。其中，涨幅最高的是上投全球天然资源股票，为15.34%；其次是华宝油气、富国全球顶级消费品股票，分别上涨14.59%和12.31%。还有招商全球资源股票、华安标普石油指数、交银全球资源股票、汇添富黄金及贵金属等涨幅都在10%以上。易方达黄金主题，诺安全球黄金的涨幅也很可观");
			
			docs.add(doc);
			if(i%1000==0){
				app.addDocuments(docs, commitWithinMs, collectionName);
				out("adddocuments: "+i);
				docs.clear();
			}
		}
		if(docs.size()>0){
			app.addDocuments(docs, commitWithinMs, collectionName);
		}
		/** -- END 插入数据 -- */
		
		app.disconnectServer();
	}
	static void out(String info) {
		System.out.println(new SimpleDateFormat("yyyyMMdd hhmmss SSS ").format(new Date())+info);
	}
	
	public CollectionManager(){
		try {
			final String defaultCollection = "som";
			final int zkClientTimeout = 20000;
			final int zkConnectTimeout = 1000;

			cloudSolrServer=new CloudSolrServer(zkHost);
			out("The Cloud SolrServer Instance has benn created!");
//
//			cloudSolrServer.setDefaultCollection(defaultCollection);
//			cloudSolrServer.setZkClientTimeout(zkClientTimeout);
//			cloudSolrServer.setZkConnectTimeout(zkConnectTimeout);
		} catch (Exception e) {//MalformedURLException
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	public int connectServer(){
		cloudSolrServer.connect();
		out("The cloud Server has been connected !!!!");
		return 0;
	}
	
	public int disconnectServer(){
		cloudSolrServer.shutdown();
		out("The cloud Server has been disconnected !!!!");
		return 0;
	}
	public void setDefaultCollection(String collectionName){
		cloudSolrServer.setDefaultCollection(collectionName);
	}
	public int addDocuments(Collection<SolrInputDocument> docs,int commitWithinMs,String collectionName){
		try {
			if(!cloudSolrServer.getDefaultCollection().equals(collectionName)){
				cloudSolrServer.setDefaultCollection(collectionName);
			}
			cloudSolrServer.add(docs, commitWithinMs);
		} catch (SolrServerException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return 0;
	}
	public int clearConfig(String collectionName){

		try {

			String[] zkparams = new String[] {
					"-cmd",
					"clear",
					"-zkhost",
					zkHost,
					baseZkSolrConfigDir+"/"+collectionName + configNameSuffix };
			ZkCLI.main(zkparams);
		} catch (Exception e) {
			log.error("zk upload conf failed !!!", e);
			return -202;
		}
		
		return 0;
	}
	public int downloadConfig(String configPath,String collectionName){
		try {

			String[] zkparams = new String[] {
					"-cmd",
					"downconfig",
					"-zkhost",
					zkHost,
					"-confdir",	configPath,
					"-confname", collectionName + configNameSuffix };
			ZkCLI.main(zkparams);
			
		} catch (Exception e) {
			log.error("zk download conf failed !!!", e);
			return -202;
		}
		
		return 0;
	}
	/**
	 * 将configPath中指定的config目录下的所有文件上传到zookeeper中的collectionName对应的目录中
	 * @param configPath
	 * @param collectionName
	 * @return
	 */
	public int uploadConfig(String configPath,String collectionName){
		if(!new File(configPath).exists()){
			log.warn("local config path ["+configPath+"] not exists!");
			return -103;
		}
		
		// 上传solr config到zookeeper,并设置别名
		try {

			String[] zkparams = new String[] {
					"-cmd",
					"upconfig",
					"-zkhost",
					zkHost,
					"-confdir",	configPath,
					"-confname", collectionName + configNameSuffix };
			ZkCLI.main(zkparams);
			
//			zkparams = new String[] { "-cmd", "linkconfig", "-zkhost", zkHost,
//					"-collection", collectionName, "-confname",collectionName + "_conf" };
//			ZkCLI.main(zkparams);
		} catch (Exception e) {
			log.error("zk upload conf failed !!!", e);
			return -202;
		}
		// //////////////////
		
		return 0;
	}
	//http://172.16.0.4:8983/solr/admin/collections?action=SPLITSHARD&collection=som&shard=shard4
	public int splitShard(String collection,String shard){
		ModifiableSolrParams params = new ModifiableSolrParams();
		SolrRequest request = new QueryRequest(params);
		request.setPath("/admin/collections");
		// new core
		params.set("action",CollectionParams.CollectionAction.SPLITSHARD.toString());
		params.set("collection", collection);
		params.set("shard", shard);
		return requestCloud(request);
	}
	public int deleteShard(String collection,String shard){
		ModifiableSolrParams params = new ModifiableSolrParams();
		SolrRequest request = new QueryRequest(params);
		request.setPath("/admin/collections");
		// new core
		params.set("action",CollectionParams.CollectionAction.DELETESHARD.toString());
		params.set("collection", collection);
		params.set("shard", shard);
		return requestCloud(request);
		
	}
	
	/**
	 * 
	 * @param collectionName
	 * @return <shardName,<propName,propValue>>
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
			out("shard "+shardName+" "+shard.getProperties());
			maps.put(shardName,entry.getValue().getProperties());
		}
		return maps;
	}
	
	public Map<String, Map<String,String>> getShards_bak(String collectionName){
		Map<String, Map<String,String>> maps = new HashMap<String, Map<String,String>>();
		ClusterState clusterState = cloudSolrServer.getZkStateReader()
				.getClusterState();
		Map<String, Slice> slices = clusterState.getSlicesMap(collectionName);
		
		if (slices == null)
			return maps;
		
		
		for (Map.Entry<String, Slice> entry : slices.entrySet()) {
			
			Slice shard=entry.getValue();
			String shardName = shard.getName();
			out("shard "+shardName+" "+shard.getProperties());
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
	public int createReplica(String replicaName,String collectionName,String shardName,String solrBasePath){
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
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return 0;
	}
	public Set<String> getCollections(){
		if(cloudSolrServer==null){
			out("cloudSolrServer is null");
			return null;
		}
		ClusterState cs = cloudSolrServer.getZkStateReader().getClusterState();

		return cs.getCollections();
	}
	
	public int requestCloud(SolrRequest request){
		//CloudSolrServer cloudServer=null;
		try {
			//cloudServer= new CloudSolrServer(zkHost);
			cloudSolrServer.request(request);
			out("request [" + request.getPath() + "]  on cloud success.");
		} catch (Exception e) {
			e.printStackTrace();
			out("request [" + request.getPath() + "]  on cloud failed! "
					+ e.getMessage());
		}
		return 0;
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
	// 获取当前存活的节点列表
	public HashMap<String,String> getLiveNodes(){
		HashMap<String,String> liveNodes=new HashMap<String, String>();

		ClusterState cs = cloudSolrServer.getZkStateReader().getClusterState();
		SolrZkClient zkClient = null;
		try {
			zkClient = new SolrZkClient(zkHost, 10000);
			Set<String> nodeNames = cs.getLiveNodes();
			for (String nn : nodeNames) {
				System.out.println("nodeName:" + nn);
				String basepath = zkClient.getBaseUrlForNodeName(nn);
				System.out.println(nn + "->urlpath:" + basepath);
				liveNodes.put(nn, basepath);
			}
		} finally {

			if (zkClient != null) {
				zkClient.close();
			}

		}
		return liveNodes;
	}
	public boolean isCollectionExist(String collectionName){
		Set<String> collections = getCollections();
		if(collections!=null&&collections.contains(collectionName)){
			return true;
		}
		return false;
	}
	public int deleteCollection(String collectionName){
		
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

	public int createCollection(String collectionName,int rf,int sn,int msp,List<String> jdlist){
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
		// defined ,default
		// all node.
		
		SolrRequest request = new QueryRequest(coreparams);
		request.setPath("/admin/collections"); // 请求位置
		
		requestCloud(request);
		return 0;
	}
	
	public int updateCollection(String collectionName){
		
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
}
/***
 * 查看节点状态
 * http://172.16.0.4:8983/solr/admin/cores?action=status
 * http://172.16.0.4:8983/solr/admin/cores?action=status&core=som_shard1_replica1
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
 * 
 * **/