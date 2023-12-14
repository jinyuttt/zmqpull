package org.example;

import org.example.zmq.DataType;
import org.example.zmq.ICallBack;
import org.example.zmq.PollSubscriber;
import org.example.zmq.ZmqProxy;
import org.zeromq.SocketType;
import org.zeromq.ZMQ;

import java.util.Random;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
//        proxy();
//        sub();;
//        push();
       test();
       suber();
       push();

    }
    public static void test()
    {
        ZmqProxy  proxy=new ZmqProxy();
        proxy.IsStore=true;
        proxy.storePath="db";
        proxy.localNetStore="127.0.0.1:7689";
        proxy.start();
    }
    public static  void  suber()
    {
        Random random=new Random(System.currentTimeMillis());
        for (int i=0;i<5;i++) {
            PollSubscriber subscriber = new PollSubscriber();
            subscriber.init("tcp://127.0.0.1:4456");
            subscriber.subscribe("ttt", new ICallBack() {
                @Override
                public void add(String topic, String client, byte[] data) {

                    System.out.println(new String(data));
                    try {
                        Thread.sleep(random.nextInt(0,100));
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            }, DataType.early);
        }
    }

    public  static void push()
    {
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        ZMQ.Socket socket= ZMQ.context(1).socket(SocketType.PUB);
        socket.connect("tcp://127.0.0.1:4455");
        while (true) {
            socket.sendMore("ttt");
           socket.sendMore("AAA");
            socket.send(String.valueOf(System.currentTimeMillis()));
//            try {
//                Thread.sleep(1000);
//            } catch (InterruptedException e) {
//                throw new RuntimeException(e);
//            }
        }
    }

    public  static void proxy()
    {
        Thread pt=new Thread(new Runnable() {
            @Override
            public void run() {
               ZMQ.Context context=ZMQ.context(1);
                ZMQ.Socket sub=context.socket(SocketType.XSUB);
                ZMQ.Socket pub=context.socket(SocketType.XPUB);
                sub.bind("tcp://127.0.0.1:4455");
                 pub.bind("tcp://127.0.0.1:4466");
                ZMQ.proxy(sub,pub,null);
            }
        });
        pt.start();
    }
    public  static void sub()
    {
        for (int i=0;i<5;i++
             ) {
            Thread pt=new Thread(new Runnable() {
                @Override
                public void run() {
                    ZMQ.Context context=ZMQ.context(1);
                    ZMQ.Socket sub=context.socket(SocketType.SUB);
                    sub.connect("tcp://127.0.0.1:4466");
                    sub.subscribe("");
                    while (true)
                    {
                        String topic=  sub.recvStr();
                        sub.recv();
                        System.out.println(topic);
                    }



                }
            });
            pt.start();
        }

    }
}
