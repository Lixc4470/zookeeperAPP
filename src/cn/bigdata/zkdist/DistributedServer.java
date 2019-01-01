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
	 *  ����zk�Ŀͻ�������
	 * @throws IOException 
	 */
	public void getConnect() throws IOException {
		zk = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
			
			@Override
			public void process(WatchedEvent event) {
				//�յ��¼�֪ͨ��Ļص�����
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
	 * ע���������Ϣ��zk
	 * @param hostname ����������
	 * @throws Exception
	 */
	public void registerServer(String hostname) throws Exception{
		String create = zk.create(parentNode+"/server", hostname.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
		System.out.println(hostname+" is online..."+create);
	}
	
	
	/**
	 * ҵ����
	 * @param hostname
	 * @throws InterruptedException 
	 */
	public void handleBussiness(String hostname) throws InterruptedException {
		System.out.println(hostname+" Start working");
		Thread.sleep(Long.MAX_VALUE);
	}
	
	
	public static void main(String[] args) throws Exception {

		DistributedServer dServer = new DistributedServer();
		//��ȡ����
		dServer.getConnect();
		
		dServer.registerServer(args[0]);
		
		dServer.handleBussiness(args[0]);
		
	}

}
