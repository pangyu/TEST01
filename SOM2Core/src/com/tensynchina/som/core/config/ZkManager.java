package com.tensynchina.som.core.config;


import java.net.MalformedURLException;

import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.zookeeper.CreateMode;

public class ZkManager {
	private String zkHost = "192.168.1.15:2181,192.168.1.16:2181,192.168.1.18:2181";
	private String baseZkSolrConfigDir = ZkController.CONFIGS_ZKNODE;
	public final static String CONFIGNAME_PROP="configName";
	static final int TIMEOUT = 10000;
	private CloudSolrServer cloudSolrServer = null;

	public ZkManager(CloudSolrServer css) {
		try {
			final String defaultCollection = "som";
			final int zkClientTimeout = 20000;
			final int zkConnectTimeout = 1000;
			if (css == null) {
				cloudSolrServer = new CloudSolrServer(zkHost);
				cloudSolrServer.setDefaultCollection(defaultCollection);
				cloudSolrServer.setZkClientTimeout(zkClientTimeout);
				cloudSolrServer.setZkConnectTimeout(zkConnectTimeout);
			} else {
				cloudSolrServer = css;
			}
		} catch (MalformedURLException e) {
			e.printStackTrace();
		}
	}

	public int connectServer() {
		cloudSolrServer.connect();
		return 0;
	}

	public int disconnectServer() {
		cloudSolrServer.shutdown();
		return 0;
	}

	public void newCfgPath(String directoryName) throws Exception {
		String dir = baseZkSolrConfigDir + "/" + directoryName;
		SolrZkClient zkClient = cloudSolrServer.getZkStateReader().getZkClient();
		if (!zkClient.exists(dir, true)) {
			zkClient.makePath(dir, false, true);
		}
	}
	public void loadCfgDatas(String directoryName,String fileName) throws Exception {
		String dir = baseZkSolrConfigDir + "/" + directoryName;
		String fn = dir+"/"+fileName;
		SolrZkClient zkClient = cloudSolrServer.getZkStateReader().getZkClient();
		out("dir:"+dir);
		if (zkClient.exists(dir, true)) {
			byte[] datas = zkClient.getData(fn, null, null, true);
			if(datas != null) {
		      out(fileName+":"+new String(datas,"utf-8"));
		    }
		} else {
			out("dir not exist.");
		}
	}
	public void deleteCfgDatas(String directoryName,String fileName) throws Exception {
		String dir = baseZkSolrConfigDir + "/" + directoryName;
		String fn = dir+"/"+fileName;
		SolrZkClient zkClient = cloudSolrServer.getZkStateReader().getZkClient();
		out("dir:"+dir);
		if (zkClient.exists(fn, true)) {
			zkClient.delete(fn, -1, true);
		} else {
			out("fn not exist.");
		}
	}
	public int uploadToCfgPath(String directoryName,String fileName,byte[] fdatas) throws Exception {
		String dir = baseZkSolrConfigDir + "/" + directoryName;
		String fn = dir + "/" + fileName;
		SolrZkClient zkClient = cloudSolrServer.getZkStateReader().getZkClient();
		if (!zkClient.exists(dir, true)) {
			zkClient.makePath(dir, false, true);
		}
		if (zkClient.exists(dir, true)) {
			out("exists!"+dir);
			zkClient.makePath(fn, fdatas,CreateMode.PERSISTENT, null, false, true);
		} else {
			return -1;
		}
		return 0;
		
	}
	static void out(String info) {
		System.out.println(info);
	}
	public static void main(String[] args) throws Exception{
		ZkManager zm = new ZkManager(null);
		zm.loadCfgDatas("tb_conf","schema.xml");
	}
}