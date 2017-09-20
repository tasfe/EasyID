package com.gome.pop.fup.easyid.server;

import com.gome.pop.fup.easyid.model.Request;
import com.gome.pop.fup.easyid.snowflake.Snowflake;
import com.gome.pop.fup.easyid.util.*;
import com.gome.pop.fup.easyid.zk.ZkClient;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import redis.clients.jedis.ShardedJedisPipeline;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.util.Iterator;
import java.util.Set;

/**
 * 服务端，接收创建id的请求
 * Created by fupeng-ds on 2017/8/2.
 */
public class Server extends Thread {

    private static final Logger logger = Logger.getLogger(Server.class);

    private Snowflake snowflake = new Snowflake();

    private ZkClient zkClient;

    private String redisAddress;

    private String zookeeperAddres;

    private JedisUtil jedisUtil;

    private Selector selector;

    private ServerSocketChannel serverSocketChannel;

    public void startup() throws Exception {
        jedisUtil = JedisUtil.newInstance(redisAddress);
        String localHost = IpUtil.getLocalHost();
        Cache.set(Constant.LOCALHOST, localHost, -1l);
        zkClient = new ZkClient(zookeeperAddres);
        zkClient.register(localHost);
        //查看redis中是否有id,没有则创建
        pushIdsInRedis();
        selector = Selector.open();
        serverSocketChannel = ServerSocketChannel.open();
        serverSocketChannel.socket().setReuseAddress(true);
        serverSocketChannel.configureBlocking(false);
        serverSocketChannel.socket().bind(new InetSocketAddress("127.0.0.1", Constant.EASYID_SERVER_PORT));
        serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            public void run() {
                zkClient.close();
                jedisUtil.close();
            }
        }));
        //启动服务
        this.start();


    }

    public void run() {
        logger.info("EasyID Server started!");
        while (true) {
            try {
                selector.select(1000);
                Set<SelectionKey> selectionKeys = selector.selectedKeys();
                Iterator<SelectionKey> iterator = selectionKeys.iterator();
                while (iterator.hasNext()) {
                    SelectionKey selectionKey = iterator.next();
                    handle(selectionKey);
                }
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (KeeperException e) {
                e.printStackTrace();
            }
        }
    }

    private void handle(SelectionKey key) throws IOException, KeeperException, InterruptedException {
        try {
            if (key.isValid()) {
                if (key.isAcceptable()) {
                    ServerSocketChannel serverSocketChannel = (ServerSocketChannel)key.channel();
                    SocketChannel socketChannel = serverSocketChannel.accept();
                    if (socketChannel != null) {
                        socketChannel.configureBlocking(false);
                        socketChannel.register(selector, SelectionKey.OP_READ);
                    }
                }
                if (key.isReadable()) {
                    SocketChannel socketChannel = (SocketChannel)key.channel();
                    ByteBuffer buffer = ByteBuffer.allocate(1024);
                    int read = socketChannel.read(buffer);
                    if (read > 0) {
                        buffer.flip();
                        byte[] bytes = new byte[buffer.remaining()];
                        buffer.get(bytes);
                        Request request = KryoUtil.byteToObj(bytes, Request.class);
                        if (request.getType() == MessageType.REQUEST_TYPE_CREATE) {
                            pushIdsInRedis();
                        }
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
            key.cancel();
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void pushIdsInRedis() throws KeeperException, InterruptedException {
        //从zookeeper中获取队列长度参数
        int redis_list_size = zkClient.getRedisListSize();
        Long len = jedisUtil.llen(Constant.REDIS_LIST_NAME);
        if (len == null) len = 0l;
        if (len.intValue() < (redis_list_size * 300)) {
            long[] ids = snowflake.nextIds((redis_list_size * 1000) - len.intValue());
            String[] strings = ConversionUtil.longsToStrings(ids);
            jedisUtil.rpush(Constant.REDIS_LIST_NAME, strings);
        }
        //删除redis锁
        //jedisUtil.del(Constant.REDIS_SETNX_KEY);
    }

    public Snowflake getSnowflake() {
        return snowflake;
    }

    public void setSnowflake(Snowflake snowflake) {
        this.snowflake = snowflake;
    }

    public ZkClient getZkClient() {
        return zkClient;
    }

    public void setZkClient(ZkClient zkClient) {
        this.zkClient = zkClient;
    }

    public String getZookeeperAddres() {
        return zookeeperAddres;
    }

    public void setZookeeperAddres(String zookeeperAddres) {
        this.zookeeperAddres = zookeeperAddres;
    }

    public String getRedisAddress() {
        return redisAddress;
    }

    public void setRedisAddress(String redisAddress) {
        this.redisAddress = redisAddress;
    }
}
