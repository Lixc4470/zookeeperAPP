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
	
	//ע��:��volatile��������ڣ�
	private volatile List<String> ServerList;
	
	/**
	 * ����zk�ͻ�������
	 * @throws Exception
	 */
	public void getConnect() throws Exception {
		zk = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
			
			@Override
			public void process(WatchedEvent event) {
				//�յ�֪ͨ��Ļص�����
				try {
					getServerList();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		});
	}
	
	/**
	 * ��ȡ��������Ϣ�б�
	 * @throws Exception
	 */
	public void getServerList() throws Exception {
		//��ȡ�������ӽڵ���Ϣ����������������
		List<String> childrens = zk.getChildren(parentNode, true);
		
		//�ȴ���һ���ֲ���List���洢��������Ϣ
		List<String> servers = new ArrayList<>();
		for(String child:childrens) {
			//child���ӽڵ�Ľڵ���
			byte[] data = zk.getData(parentNode+"/"+child, false, null);
			servers.add(new String(data));
		}
		
		//�ѷ�������Ϣ��ֵ��ServerList
		ServerList = servers;
		System.out.println(ServerList);
	}
	
	/**
	 * ҵ����
	 * @throws InterruptedException
	 */
	public void handleBussiness() throws InterruptedException {
		System.out.println("client start working...");
		Thread.sleep(Long.MAX_VALUE);
	}
	
	public static void main(String[] args) throws Exception {
		
		DistributedClient client = new DistributedClient();
		//��ȡzk�ͻ�������
		client.getConnect();
		
		client.getServerList();
		
		client.handleBussiness();
	}

}
