package cn.bigdata.zkdist;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

public class DistributedClient {

	private static final String connectString = "min1:2181,min2:2181,min3:2181";
	private static final int sessionTimeout = 2000;
	private ZooKeeper zk = null;
	private static final String parentNode="/servers";
	
	//注意:加volatile的意义何在？
	private volatile List<String> ServerList;
	
	/**
	 * 创建zk客户端连接
	 * @throws Exception
	 */
	public void getConnect() throws Exception {
		zk = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
			
			@Override
			public void process(WatchedEvent event) {
				//收到通知后的回调函数
				try {
					getServerList();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		});
	}
	
	/**
	 * 获取服务器信息列表
	 * @throws Exception
	 */
	public void getServerList() throws Exception {
		//获取服务器子节点信息，并监听父服务器
		List<String> childrens = zk.getChildren(parentNode, true);
		
		//先创建一个局部的List来存储服务器信息
		List<String> servers = new ArrayList<>();
		for(String child:childrens) {
			//child是子节点的节点名
			byte[] data = zk.getData(parentNode+"/"+child, false, null);
			servers.add(new String(data));
		}
		
		//把服务器信息赋值个ServerList
		ServerList = servers;
		System.out.println(ServerList);
	}
	
	/**
	 * 业务功能
	 * @throws InterruptedException
	 */
	public void handleBussiness() throws InterruptedException {
		System.out.println("client start working...");
		Thread.sleep(Long.MAX_VALUE);
	}
	
	public static void main(String[] args) throws Exception {
		
		DistributedClient client = new DistributedClient();
		//获取zk客户端连接
		client.getConnect();
		
		client.getServerList();
		
		client.handleBussiness();
	}

}
