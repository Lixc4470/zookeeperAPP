package cn.bigdata.zk;

import java.io.IOException;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.junit.Before;
import org.junit.Test;

import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;


public class SimpleZkClient {

	private static final String connectString="min1:2181,min2:2181,min3:2181";
	private static final int sessionTimeout = 2000;
	ZooKeeper zKeeper = null;
	
	@Before
	public void init() throws IOException {
		zKeeper=new ZooKeeper(connectString, sessionTimeout, new Watcher() {
			
			@Override
			public void process(WatchedEvent event) {
				System.out.println(event.getType()+"---"+event.getPath());
				
				try {
					zKeeper.getChildren("/", true);
				} catch (KeeperException e) {
					e.printStackTrace();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		});
	}



	public void testCreate() throws KeeperException, InterruptedException {
		String createNode = zKeeper.create("/eclipse2", "eclipseTest2".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		System.out.println(createNode);
	}



	/**
	 * 判断节点是否存在
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	public void testExists() throws KeeperException, InterruptedException {
		
		Stat exists = zKeeper.exists("/eclipse", true);
//		System.out.println(exists);
		System.out.println(exists==null?"not exists":"exists");
	}
	
	
	/**
	 * 获取子节点
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	public void getChildrens() throws KeeperException, InterruptedException {
		List<String> childrens = zKeeper.getChildren("/", true);
		for(String child:childrens) {
			System.out.println(child);
		}
		Thread.sleep(Long.MAX_VALUE);
	}
	
	/**
	 * 获取节点数据
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	public void getData() throws KeeperException, InterruptedException {
		byte[] data = zKeeper.getData("/eclipse", false, null);
		System.out.println(new String(data));
	}
	
	/**
	 * 删除节点
	 * @throws InterruptedException
	 * @throws KeeperException
	 */
	public void deleteNode() throws InterruptedException, KeeperException {
		zKeeper.delete("/eclipse2", -1);
	}
	
	@Test
	/**
	 * 设置节点的数据
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	public void setData() throws KeeperException, InterruptedException {
		Stat setData = zKeeper.setData("/eclipse", "hellozk111".getBytes(), -1);
//		System.out.println(setData.toString());
		byte[] data = zKeeper.getData("/eclipse", false, null);
		System.out.println(new String(data));
	}
}
