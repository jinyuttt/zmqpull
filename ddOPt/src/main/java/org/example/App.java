package org.example;

import org.example.zmq.*;
import org.zeromq.SocketType;
import org.zeromq.ZMQ;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Random;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {

       test();
       suber();
       dbtopic();
       push();
     //   DBtest();

    }
    static void DBtest()
    {
        BDBLocalUtility bdbLocalUtility=new BDBLocalUtility("zmq","pull");
        bdbLocalUtility.init();
        for (int i=0;i<1000;i++
             ) {
            bdbLocalUtility.put("ttt",String.valueOf(System.currentTimeMillis()),false);
        }
      //  bdbLocalUtility.delete(100);
       long count= bdbLocalUtility.truncateDB();
        System.out.println(count);
    }

    static void  dbtopic()
    {
        Thread test=new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    Thread.sleep(10000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                ZMQ.Socket socket= ZMQ.context(1).socket(SocketType.PUB);
                socket.connect("tcp://127.0.0.1:4455");
                // while (true) {
                socket.sendMore(DBOpt.QueryDataDB.name());
                socket.sendMore("AAA");
                socket.send("ttt");

                socket.sendMore(DBOpt.DeleteDataDBMis.name());
                socket.sendMore("AAA");
                byte[] bytes="ttt".getBytes(StandardCharsets.UTF_8);
                ByteBuffer buffer=ByteBuffer.allocate(bytes.length+8);
                buffer.putLong(1000);
                buffer.put(bytes);
                socket.send(buffer.array());

                socket.sendMore(DBOpt.DeleteDataDB.name());
                socket.sendMore("AAA");
                socket.send("ttt");

                socket.sendMore(DBOpt.ClearDataDB.name());
                socket.sendMore("AAA");
                socket.send("ttt");

            }
        });
        test.start();

    }
    public static  void unsuber(PollSubscriber subscriber)
    {
        Thread ss=new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    Thread.sleep(50000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                subscriber.unsubscribe("ttt");
            }
        });
        ss.start();
    }
    public static  void testdb(ZmqProxy proxy)
    {
        Thread ss=new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    Thread.sleep(50000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                proxy.delete(10000);
            }
        });
        ss.start();
    }
    public static void test()
    {
        ZmqProxy  proxy=new ZmqProxy();
        proxy.IsStore=true;
        proxy.storePath="db";
        proxy.localNetStore="127.0.0.1:7689";
        proxy.start();
      // testdb(proxy);
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

                    System.out.println(topic+"_"+new String(data));
                    try {
                        Thread.sleep(random.nextInt(0,100));
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            }, DataType.early);
            if(i==0)
            {
                unsuber(subscriber);
            }
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
            socket.send(String.valueOf("fff"+System.currentTimeMillis()));
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
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
