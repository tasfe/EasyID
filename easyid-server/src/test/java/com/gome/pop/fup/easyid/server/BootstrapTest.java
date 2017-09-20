package com.gome.pop.fup.easyid.server;

/**
 * Created by fupeng-ds on 2017/8/4.
 */
public class BootstrapTest {

    public static void main(String[] args) throws Exception {
        args = new String[]{"-zookeeper192.168.56.102:2181", "-redis192.168.56.102:6379"};
        Bootstrap bootstrap = new Bootstrap();
        bootstrap.main(args);
    }
}
