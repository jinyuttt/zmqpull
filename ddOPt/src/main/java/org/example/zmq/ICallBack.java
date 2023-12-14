package org.example.zmq;


/**
 * 订阅回调
 */
public interface ICallBack {

    /**
     * 加入数据
     * @param topic 主题
     * @param client  客户端
     * @param data 数据
     */
    public void add(String topic,String client,byte[]data);
}
