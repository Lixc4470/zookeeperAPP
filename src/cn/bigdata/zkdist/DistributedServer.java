package cn.bigdata.zkdist;

import java.io.IOException;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

public class DistributedServer {

	private final static String connectString = "min2:2181,min1:2181,min3:2181";
	private final static int sessionTimeout = 2000;
	private ZooKeeper zk = null;
	private final static String parentNode = "/servers";
	
	/**
	 *  创建zk的客户端连接
	 * @throws IOException 
	 */
	public void getConnect() throws IOException {
		zk = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
			
			@Override
			public void process(WatchedEvent event) {
				//收到事件通知后的回调函数
				System.out.println(event.getType()+"---"+event.getPath());
				
				try {
					zk.getChildren("/", true);
				} catch (KeeperException e) {
					e.printStackTrace();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		});
	}
	
	/**
	 * 注册服务器信息到zk
	 * @param hostname 服务器名称
	 * @throws Exception
	 */
	public void registerServer(String hostname) throws Exception{
		String create = zk.create(parentNode+"/server", hostname.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
		System.out.println(hostname+" is online..."+create);
	}
	
	
	/**
	 * 业务功能
	 * @param hostname
	 * @throws InterruptedException 
	 */
	public void handleBussiness(String hostname) throws InterruptedException {
		System.out.println(hostname+" Start working");
		Thread.sleep(Long.MAX_VALUE);
	}
	
	
	public static void main(String[] args) throws Exception {

		DistributedServer dServer = new DistributedServer();
		//获取连接
		dServer.getConnect();
		
		dServer.registerServer(args[0]);
		
		dServer.handleBussiness(args[0]);
		
	}

}
