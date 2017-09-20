package com.gome.pop.fup.easyid.demo;

import com.gome.pop.fup.easyid.id.EasyID;
/**
 * Created by fupeng-ds on 2017/8/4.
 */
public class Main {

    public static void main(String[] args) {
        final EasyID easyID = new EasyID("192.168.56.102:2181", "192.168.56.102:6379");
        //final JedisUtil jedisUtil = JedisUtil.newInstance("192.168.56.102", 6379);
        for (int i = 0; i < 1500; i++) {
            Thread thread = new Thread(new Runnable() {
                public void run() {
                    long id = 0;
                    try {
                        id = easyID.nextId();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    System.out.println("EasyID nextId : " + id + ",time:" + System.currentTimeMillis());
                    //jedisUtil.incr("count");
                }
            });
            thread.start();
        }
    }


}
