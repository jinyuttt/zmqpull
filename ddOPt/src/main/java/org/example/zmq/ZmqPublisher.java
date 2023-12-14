package org.example.zmq;

import org.zeromq.SocketType;
import org.zeromq.ZMQ;

public class ZmqPublisher {
    ZMQ.Socket socket=null;

    public String Client="A";
    public  void  init(String address)
    {
        socket=ZMQ.context(1).socket(SocketType.PUB);
        socket.connect(address);
    }

    public  void  publish(String topic,byte[]data)
    {
        socket.sendMore(topic);
        socket.sendMore(Client);
        socket.send(data);
    }

}
