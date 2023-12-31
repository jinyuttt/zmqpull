package org.example.zmq;

import org.zeromq.ZMQ;

import java.util.concurrent.locks.ReentrantLock;

/**
 * push结构
 */
class PushSocket {
   private ReentrantLock  reentrantLock=new ReentrantLock();

    /**
     * IP地址
     */
    public String ip;

    /**
     * 端口
     */
    public  int port;

    /**
     * 对应主题
     */
    public  String topic;

    private ZMQ.Socket socket;

    /**
     * 分组标识
     */
    public String identy;



    PushSocket(ZMQ.Socket socket) {
        this.socket = socket;
    }

    /**
     * 返回地址
     * @return
     */
    public  String toAddress()
    {
        return  ip+":"+port;
    }

    /**
     * 发数据
     * @param topic
     * @param client
     * @param data
     * @return
     */
    public boolean  send(String topic,String client,byte[]data)
    {
       if(reentrantLock.tryLock()) {
          try {
             socket.sendMore(topic);
             socket.sendMore(client);
             socket.send(data);
          }
          finally {
             reentrantLock.unlock();
          }
          return true;
       }
       return false;
    }

    public void  close()
    {
        try {
            reentrantLock.lock();
            socket.close();
        }
        finally {
            reentrantLock.unlock();
        }
    }
}
