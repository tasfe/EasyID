package com.gome.pop.fup.easyid.id;

import com.gome.pop.fup.easyid.model.Request;
import com.gome.pop.fup.easyid.util.*;
import com.gome.pop.fup.easyid.zk.ZkClient;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import redis.clients.jedis.ShardedJedis;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;

/**
 * 客户端ID生成类
 * Created by fupeng-ds on 2017/8/3.
 */
public class EasyID {

    private static final Logger logger = Logger.getLogger(EasyID.class);

    private LinkedBlockingQueue<Request> queue = new LinkedBlockingQueue<Request>();

    private Selector selector;

    private Map<String, SocketChannel> channelPool = new ConcurrentHashMap<String, SocketChannel>();

    /**
     * 向服务端发送指令的线程，消费queue中的消息
     */
    private SendThread sendThread;

    /**
     * 服务端开始生成新的ID的开关
     */
    private volatile boolean flag = false;

    private ExecutorService executorService = Executors.newFixedThreadPool(16);

    private ZkClient zkClient;

    private JedisUtil jedisUtil;

    /**
     *ZooKeeper服务地址
     */
    private String zkAddress;

    /**
     *rediss服务地址
     */
    private String redisAddress;

    public EasyID(String zkAddress, String redisAddress) {
        //初始化ZkClient
        try {
            this.zkClient = new ZkClient(zkAddress);
            selector = Selector.open();
        } catch (IOException e) {
            e.printStackTrace();
            logger.error(e.getMessage());
        } catch (KeeperException e) {
            e.printStackTrace();
            logger.error(e.getMessage());
        } catch (InterruptedException e) {
            e.printStackTrace();
            logger.error(e.getMessage());
        }
        this.jedisUtil = JedisUtil.newInstance(redisAddress);
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            public void run() {
                zkClient.close();
                jedisUtil.close();
                executorService.shutdown();
            }
        }));
        //启动发送线程
        sendThread = new SendThread();
        sendThread.start();

    }

    /**
     * 获取id
     * @return
     */
    public long nextId() throws InterruptedException {
        long begin = System.currentTimeMillis();
        int list_min_size = zkClient.getRedisListSize() * 300;
        String id = "";
        ShardedJedis jedis = jedisUtil.getJedis();
        long len = 0l;
        try {
            len = jedis.llen(Constant.REDIS_LIST_NAME);
            if ((int) len < list_min_size) {
                //getRedisLock(jedis);
                //if (jedis.setnx(Constant.REDIS_SETNX_KEY, "1") == 1l) {
                    //jedis.expire(Constant.REDIS_SETNX_KEY, 3);
                    Request request = new Request();
                    request.setType(MessageType.REQUEST_TYPE_CREATE);
                    getSocketChannel();
                    //将请求放入队列，等待被消费
                    queue.offer(request);
                    selector.wakeup();
                }
            //}
            id = jedis.lpop(Constant.REDIS_LIST_NAME);
        } finally {
            jedisUtil.returnResource(jedis);
        }
        if (len == 0l || null == id || "".equals(id)) {
            Thread.sleep(100l);
            return nextId();
        }
        //logger.info("nextId use time : " + (System.currentTimeMillis() - begin));
        System.out.println("nextId use time : " + (System.currentTimeMillis() - begin));
        return Long.valueOf(id);
    }

    private class SendThread extends Thread {

        @Override
        public void run() {
            while (true) {
                try {
                    selector.select(1000);
                    //从队列中获取请求信息；若队列为空，则一直阻塞
                    Request request = queue.poll();
                    if (request != null) {
                        Set<SelectionKey> selectionKeys = selector.selectedKeys();
                        Iterator<SelectionKey> iterator = selectionKeys.iterator();
                        while (iterator.hasNext()) {
                            SelectionKey key = iterator.next();
                            if (key.isValid()) {
                                if (key.isConnectable()) {
                                    SocketChannel socketChannel = (SocketChannel) key.channel();
                                    if (socketChannel.finishConnect()) {
                                        //将请求写入缓存
                                        ByteBuffer buffer = ByteBuffer.wrap(KryoUtil.objToByte(request));
                                        socketChannel.write(buffer);
                                        buffer.clear();
                                    }
                                }
                            }
                        }
                    }

                } catch (IOException e) {
                    e.printStackTrace();
                    logger.error(e.getMessage());
                }
            }
        }
    }

    private SelectionKey getSocketChannel() {
        SelectionKey key = null;
        try {
            //String ip = zkClient.balance();
            String ip = "127.0.0.1";
            String host = IpUtil.getHost(ip);
            SocketChannel socketChannel = channelPool.get(host);
            if (socketChannel == null) {
                InetSocketAddress inetSocketAddress = new InetSocketAddress(host, Constant.EASYID_SERVER_PORT);
                socketChannel = SocketChannel.open();
                socketChannel.socket().setReuseAddress(true);
                socketChannel.configureBlocking(false);
                channelPool.put(host, socketChannel);
                socketChannel.connect(inetSocketAddress);
                selector.wakeup();
                key = socketChannel.register(selector, SelectionKey.OP_CONNECT);
            }

        } catch (IOException e) {
            e.printStackTrace();
            logger.error(e.getMessage());
        }
        return key;
    }

    public boolean isFlag() {
        return flag;
    }

    public void setFlag(boolean flag) {
        this.flag = flag;
    }

    public ZkClient getZkClient() {
        return zkClient;
    }

    public void setZkClient(ZkClient zkClient) {
        this.zkClient = zkClient;
    }

    public String getZkAddress() {
        return zkAddress;
    }

    public void setZkAddress(String zkAddress) {
        this.zkAddress = zkAddress;
    }

    public String getRedisAddress() {
        return redisAddress;
    }

    public void setRedisAddress(String redisAddress) {
        this.redisAddress = redisAddress;
    }

}
