package org.example.zmq;

import org.zeromq.SocketType;
import org.zeromq.ZMQ;

import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 订阅方
 */
public class PollSubscriber {

    /**
     * 分组标识
     */
    public  String indety="AA";

    /**
     * 速率计算 秒
     */
    public  int interval=10;
    ZMQ.Socket socket=null;
    ZMQ.Context context=null;

    /**
     * 订阅地址
     */
    String remoteAddress;

    /**
     * 回调
     */
    ConcurrentHashMap<String,ICallBack> map=new ConcurrentHashMap<>();
    ConcurrentHashMap<String,String> mapAddress=new ConcurrentHashMap<>();
    ConcurrentSkipListSet<String> resetSet=new ConcurrentSkipListSet<>();
    Lock lock = new ReentrantLock();
    volatile boolean isInit=true;

    private  String client="";
    private Executor executor= Executors.newCachedThreadPool();

    private  RecvicePack packRate=new RecvicePack();

    /**
     * 初始化
     * @param address
     */
    public  void init(String address)
    {
        if(socket==null) {
            context=ZMQ.context(1);
            socket =context.socket(SocketType.PULL);

        }
        remoteAddress=address;
        Random random=new Random();
        client=String.valueOf(random.nextLong(10000,Long.MAX_VALUE));
        computer();
    }

    /**
     * 计算速率
     */
    private  void  computer()
    {
        executor.execute(new Runnable() {
            @Override
            public void run() {
                while (true)
                {
                    try {
                        Thread.sleep(interval*1000);
                    } catch (InterruptedException e) {

                    }
                    for (String topic:mapAddress.keySet()
                         ) {
                        report(topic);
                    }
                    for (String topic:resetSet
                         ) {
                        resetConnect(topic);
                    }
                }
            }
        });

    }

    /**
     * 上报速率
     * @param topic
     */
    private  void  report(String topic)
    {
        int rate=packRate.getRate(topic)/interval;
        ZMQ.Socket req=context.socket(SocketType.REQ);
        req.connect(remoteAddress);
        req.sendMore(topic);//主题
        req.sendMore(DataType.last.name());//数据类型
        req.sendMore(indety);//分组标识
        req.sendMore("2");//1=注册，2=上报频率
        req.sendMore(String.valueOf(rate));//频率值
        req.send(client);//客户端
        String rateGroup= req.recvStr();
        req.close();
        String[]info=rateGroup.split("_");
        System.out.println(rateGroup);
       if(Integer.valueOf(info[0])>1)
       {
           int max=Integer.valueOf(info[1]);
           int min=Integer.valueOf(info[2]);
           if (Math.abs(min-1000)>rate)
           {
               String addr=mapAddress.getOrDefault(topic,"");
               socket.disconnect(addr);
               resetSet.add(topic);
               System.out.println("断开："+client);
           }
       }
    }

    /**
     * 重连
     * @param topic
     */
    private  void  resetConnect(String topic)
    {
      String address=   mapAddress.getOrDefault(topic,"");
      socket.connect(address);
      resetSet.remove(topic);
      System.out.println("连接:"+client);
    }
    /**
     * 订阅主题
     * @param topic
     * @param callBack
     * @param dataType
     */
    public  void  subscribe(String topic,ICallBack callBack,DataType dataType)
    {
        ZMQ.Socket req=context.socket(SocketType.REQ);
        req.connect(remoteAddress);
        req.sendMore(topic);
        req.sendMore(dataType.name());
        req.sendMore(indety);
        req.sendMore("1");//1=注册，2=上报频率
        req.sendMore("0");//频率值
        req.send(client);//客户端
       String addr= req.recvStr();
       if(!addr.startsWith("tcp"))
       {
           addr="tcp://"+addr;
       }
       socket.connect(addr);
       req.close();
       map.put(topic,callBack);
        mapAddress.put(topic,addr);
       if(isInit)
       {
           isInit=false;
           start();
       }

    }

    /**
     * 接收数据
     */
    private void  start()
    {
        if(lock.tryLock()) {//只有一个线程执行，其余返回
            Thread process = new Thread(new Runnable() {
                @Override
                public void run() {
                    recvice();
                }
            });
            process.start();
            lock.unlock();
        }
    }
    private void  recvice()
    {
        while (true) {
            String topic = socket.recvStr();
            String client = socket.recvStr();
            byte[] data = socket.recv();
            packRate.add(topic);
            ICallBack callBack = map.getOrDefault(topic, null);
            if(callBack!=null)
            callBack.add(topic, client, data);
        }
    }
}
