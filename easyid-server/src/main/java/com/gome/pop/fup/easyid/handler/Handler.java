package com.gome.pop.fup.easyid.handler;

import com.gome.pop.fup.easyid.model.Request;
import com.gome.pop.fup.easyid.server.Server;
import com.gome.pop.fup.easyid.snowflake.Snowflake;
import com.gome.pop.fup.easyid.util.*;
import com.gome.pop.fup.easyid.zk.ZkClient;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.apache.log4j.Logger;

/**
 * 请求处理handler
 * Created by fupeng-ds on 2017/8/3.
 */
public class Handler extends SimpleChannelInboundHandler<Request> {

    private static final Logger LOGGER = Logger.getLogger(Handler.class);

    private JedisUtil jedisUtil;

    private Snowflake snowflake;

    private ZkClient zkClient;

    private Server server;

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Request request) throws Exception {
        if (request.getType() == MessageType.REQUEST_TYPE_CREATE) {
            long begin = System.currentTimeMillis();
            try {
                server.pushIdsInRedis();
                LOGGER.info("handler run time:" + (System.currentTimeMillis() - begin));
            } finally {
                jedisUtil.del(Constant.REDIS_SETNX_KEY);
                //jedisUtil.returnResource(jedis);
                //zkClient.decrease(ip);
                ctx.writeAndFlush("").addListener(ChannelFutureListener.CLOSE);
            }
        }
    }

    public Handler(Server server, JedisUtil jedisUtil, Snowflake snowflake, ZkClient zkClient) {
        this.server = server;
        this.jedisUtil = jedisUtil;
        this.snowflake = snowflake;
        this.zkClient = zkClient;
    }

    public JedisUtil getJedisUtil() {
        return jedisUtil;
    }

    public void setJedisUtil(JedisUtil jedisUtil) {
        this.jedisUtil = jedisUtil;
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

}
