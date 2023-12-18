package org.example.zmq;

import org.zeromq.SocketType;
import org.zeromq.ZMQ;


/**
 * 发布数据
 */
public class SockPublisher {
    private  String address="";

    public String client="Zmq";

    private ZMQ.Socket socket=null;
    private ZMQ.Context context=ZMQ.context(1);
    public SockPublisher(String remoAddress)
    {
        address=remoAddress;
    }

    private synchronized void init()
    {
        if(socket==null) {
            socket = context.socket(SocketType.PUB);
            socket.setHWM(200000);
            socket.connect(address);
            socket.sendMore("zmqtestconnect");
            socket.sendMore(client);
            socket.send("ssss");
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * 发送数据
     * @param topic
     * @param data
     */
    public  void publish(String topic,byte[]data)
    {
        if(socket==null)
        {
           init();
        }
        socket.sendMore(topic);
        socket.sendMore(client);
        socket.send(data);
    }
    public  void publish(String topic,String data)
    {
        if(socket==null)
        {
           init();
        }
        socket.sendMore(topic);
        socket.sendMore(client);
        socket.send(data);
    }

}
