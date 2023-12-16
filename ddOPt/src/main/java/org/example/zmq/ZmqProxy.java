package org.example.zmq;

import HessianObj.HessianSerialize;
import HessianObj.SerializeFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zeromq.SocketType;
import org.zeromq.ZMQ;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ZmqProxy {
    Logger logger = LogManager.getLogger(BDBLocalUtility.class);
    private static final int  maxNum= 10000;//大于时分发
    private static final int  maxThNum= 10;//最大线程数

    private  static final  long maxFileSize=10*1024*1024*1024;

    /**
     * 订阅地址
     */
    public String SubAddress="tcp://127.0.0.1:4455";

    /**
     * 发布地址
     */
    public  String PubAddress="tcp://127.0.0.1:4456";

    /**
     * 中间地址，不用设置
     */
    public  String RouterAddress="inproc://pullproxy";


    /**
     * 数据集群交互地址
     */
    public  String storePeer="";

    /**
     * 本节点存储网络地址
     */
    public  String localNetStore="";

    /**
     * 存储路径
     */
    public String storePath="zmqdb";

    /**
     * 是否存储数据
     */
    public boolean IsStore=false;

    /**
     * 单存储还是集群
     */
    public  boolean islocalDB=true;

    /**
     * 分包线程数
     */
    public  int packThredNum=10;

    private AtomicInteger  threadnum=new AtomicInteger(0);//当前线程数
   private AtomicInteger  dtanum=new AtomicInteger(0);//数据包
    private  String loclIP="";//解析本地IP

   private ZMQ.Context context=ZMQ.context(2);
    private ConcurrentHashMap<String,PushSocket> map=new ConcurrentHashMap<>();//push管理

    private ConcurrentHashMap<String,Integer> mapRate=new ConcurrentHashMap<>();//上报的速率

    private ConcurrentHashMap<String,Integer> mapNum=new ConcurrentHashMap<>();//订阅客户端个数

    private LinkedBlockingQueue<InnerData> queue=new LinkedBlockingQueue<>();//数据
    private Executor executor= Executors.newCachedThreadPool();

   private IBDBUtil bdbOperatorUtil=null;//数据库操作

    private SerializeFactory serializeFactory=new HessianSerialize();
    /**
     * 开启
     */
    public  void  start()
    {
         register();
         init();
         proxy();
         process();
         if(IsStore&&!islocalDB&&!localNetStore.isEmpty())
         {
             BDBReplicatedUtil util=new BDBReplicatedUtil(storePath,"zmqpuller");
             util.loalAddress=localNetStore;
             if(storePeer.isEmpty())
             {
                 storePeer=localNetStore;
             }
             util.peer=storePeer;
             util.init();
             bdbOperatorUtil=util;
         }
         else if(IsStore&&islocalDB)
         {
             BDBLocalUtility util=new BDBLocalUtility(storePath,"zmqpuller");
             util.init();
             bdbOperatorUtil=util;

         }
         else if(IsStore)
         {
             System.out.println("没有设置存储网络地址localNetStore或设置为本地存储islocalDB");
             logger.info("没有设置存储网络地址localNetStore或设置为本地存储islocalDB");
         }
         if(bdbOperatorUtil!=null)
         {
             dbopt();
         }
    }

    /**
     * 初始化代理
     */
    private   void  init()
    {
        Thread pt=new Thread(new Runnable() {
            @Override
            public void run() {

                ZMQ.Socket sub=context.socket(SocketType.XSUB);
                ZMQ.Socket pub=context.socket(SocketType.XPUB);
                if(!SubAddress.startsWith("tcp"))
                {
                    SubAddress="tcp://"+SubAddress;
                }
                if(!RouterAddress.startsWith("inproc"))
                {
                    RouterAddress="inproc://"+RouterAddress;
                }
                sub.bind(SubAddress);

               int p= pub.bindToRandomPort("tcp://127.0.0.1");
               RouterAddress="tcp://127.0.0.1:"+p;
                ZMQ.proxy(sub,pub,null);
            }
        });
      pt.start();
    }


    private void  dbopt()
    {
        Thread dbopt=new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    Thread.sleep(3000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                ZMQ.Socket sub=context.socket(SocketType.SUB);
                boolean isc=  sub.connect(RouterAddress);
                sub.subscribe(DBOpt.ClearDataDB.name());
                sub.subscribe(DBOpt.DeleteDataDB.name());
                sub.subscribe(DBOpt.DeleteDataDBMis.name());
                sub.subscribe(DBOpt.QueryDataDB.name());
                while (true) {
                    String topic = sub.recvStr();
                    String client = sub.recvStr();
                    byte[] data = sub.recv();
                    if (topic.equals(DBOpt.ClearDataDB.name())) {
                        if (bdbOperatorUtil != null) {
                            bdbOperatorUtil.truncateDB();
                        }
                        continue;
                    }
                    if (topic.equals(DBOpt.DeleteDataDB.name())) {
                        if (bdbOperatorUtil != null) {
                            bdbOperatorUtil.delete(new String(data));
                        }
                        continue;
                    }
                    if (topic.equals(DBOpt.DeleteDataDBMis.name())) {
                        if (bdbOperatorUtil != null) {
                            ByteBuffer buffer = ByteBuffer.wrap(data);
                            long dt = buffer.getLong();
                            byte[] dst = new byte[data.length - 8];
                            buffer.get(dst);
                            bdbOperatorUtil.delete(new String(dst), dt);
                        }
                        continue;
                    }
                    if (topic.equals(DBOpt.QueryDataDB.name())) {
                        logger.info("处理数据库查询");
                        if (bdbOperatorUtil != null) {
                            List<byte[]> lst = bdbOperatorUtil.getDatas(new String(data));
                            if (lst.isEmpty()) {
                                lst.add(new byte[0]);
                            }
                            //查询数据返回
                            logger.info("数据库查询返回");
                            byte[] buf = serializeFactory.serialize(lst);
                            queue.add(new InnerData(DBOpt.AckQueryData.name(), "Prxy", buf));
                        }
                        continue;
                    }
                }
            }
        });
        dbopt.start();
    }
    private void proxy()
    {
        Thread subt=new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    Thread.sleep(3000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                ZMQ.Socket sub=context.socket(SocketType.SUB);
               boolean isc=  sub.connect(RouterAddress);
               sub.subscribe("");
               //
                HashSet<String> set=new HashSet<>();
                set.add(DBOpt.ClearDataDB.name());
                set.add(DBOpt.DeleteDataDB.name());
                set.add(DBOpt.DeleteDataDBMis.name());
                set.add(DBOpt.QueryDataDB.name());
                while (true)
                {
                    String topic= sub.recvStr();
                    String client=sub.recvStr();
                    byte[] data=sub.recv();

                  if(set.contains(topic))
                  {
                      continue;//另外的线程处理
                  }
                    InnerData innerData=new InnerData(topic,client,data);
                    queue.add(innerData);
                    if(dtanum.incrementAndGet()>maxNum&&threadnum.get()<maxThNum)
                    {
                        threadnum.incrementAndGet();
                        executor.execute(new Runnable() {
                            @Override
                            public void run() {
                                subPack();
                            }
                        });
                    }
                    if(bdbOperatorUtil!=null)
                    {
                        bdbOperatorUtil.put(topic,data,false);

                    }
                }
            }
        });
       subt.start();
    }

    /**
     * 注册主题
     */
    private  void register()
    {
        Thread reg=new Thread(new Runnable() {
            @Override
            public void run() {
                ZMQ.Socket socket=context.socket(SocketType.REP);
                if(!PubAddress.startsWith("tcp"))
                {
                    PubAddress="tcp://"+PubAddress;
                }
                socket.bind(PubAddress);
                while (true)
                {
                    String topic=socket.recvStr();
                    String dtype=socket.recvStr();
                    String indenty=socket.recvStr();
                    String isreg=socket.recvStr();
                    String rate=socket.recvStr();
                    String client=socket.recvStr();
                    String key=topic+"_"+indenty;
                    if(isreg.equals("1")) {//注册
                        PushSocket pushSocket = map.getOrDefault(key, null);
                        if (pushSocket == null) {
                            pushSocket = createPush(topic, indenty);
                            map.put(key, pushSocket);
                        }
                      int num=  mapNum.getOrDefault(key,0);
                        mapNum.put(key,num+1);
                        socket.send(pushSocket.ip + ":" + pushSocket.port);
                        if (DataType.valueOf(dtype) == DataType.early && IsStore) {
                            PushSocket finalPushSocket = pushSocket;
                            executor.execute(new Runnable() {
                                @Override
                                public void run() {
                                    List<byte[]> list = bdbOperatorUtil.getDatas(topic);
                                    for (int i = 0; i < list.size(); i++) {

                                        while (!finalPushSocket.send(topic,"",list.get(i)))
                                        {
                                            //这里必须成功
                                        }
                                    }
                                }
                            });
                        }
                    }
                    else if(isreg.equals("2")) {
                        String keytmp = key + "_" + client;
                        mapRate.put(keytmp, Integer.valueOf(rate));
                        int max = 0;
                        int min = 0;
                        for (var enty : mapRate.entrySet()) {
                            if (enty.getKey().startsWith(key)) {
                                if (max < enty.getValue()) {
                                    max = enty.getValue();
                                }
                                if (min < enty.getValue()) {
                                    min = enty.getValue();
                                }
                            }
                        }
                        //
                        int count = mapNum.getOrDefault(key, 0);
                        String rsp = count + "_" + max + "_" + min;
                        socket.send(rsp);
                    }
                    else if(isreg.equals("3"))
                    {
                        //修改个数
                        int count = mapNum.getOrDefault(key, 0);
                        mapNum.put(key, count-1);
                        //移除频率
                        String keytmp = key + "_" + client;
                        mapRate.remove(keytmp);
                        //释放资源
                        if(count-1==0)
                        {
                          PushSocket pushSocket=  map.getOrDefault(topic,null);
                          if(pushSocket!=null)
                          {
                              pushSocket.close();
                          }
                        }
                        socket.send("ok!");
                        System.out.println("注销："+client);

                    }
                }
            }
        });
        reg.start();

    }

    /**
     * 处理数据
     */
    private  void  process()
    {
        Thread pro=new Thread(new Runnable() {
            @Override
            public void run() {
                while (true)
                {
                    try {
                        InnerData data= queue.take();
                        dtanum.decrementAndGet();
                        Iterator<Map.Entry<String, PushSocket>> iterator = map.entrySet().iterator();

                        while (iterator.hasNext()) {

                            Map.Entry<String, PushSocket> entry = iterator.next();
                            if(entry.getKey().startsWith(data.topic))
                            {
                                if(!entry.getValue().send(data.topic,data.client,data.data))
                                {
                                    //没有使用则重新入队
                                    queue.put(data);
                                }
                            }

                        }

                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        });
       pro.start();
    }

    /**
     * 线程分发
     */
    private void  subPack()
    {
        System.out.println("启动分发");
        while (!queue.isEmpty())
        {
            try {
                InnerData data= queue.poll();
                if(data==null)
                {
                    //没有数据就退出
                    break;
                }
                dtanum.decrementAndGet();
                Iterator<Map.Entry<String, PushSocket>> iterator = map.entrySet().iterator();

                while (iterator.hasNext()) {

                    Map.Entry<String, PushSocket> entry = iterator.next();
                    if(entry.getKey().startsWith(data.topic))
                    {
                        if(!entry.getValue().send(data.topic,data.client,data.data))
                        {
                            queue.put(data);
                        }

                    }

                }

            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        threadnum.decrementAndGet();
    }

    /**
     * 创建push
     * @param topic
     * @param indey
     * @return
     */
    private  PushSocket  createPush(String topic,String indey)
    {
        if(loclIP=="") {
            //提取IP
            String regx = "((2(5[0-5]|[0-4]\\d))|[0-1]?\\d{1,2})(\\.((2(5[0-5]|[0-4]\\d))|[0-1]?\\d{1,2})){3}";
            Pattern pattern = Pattern.compile(regx);
            Matcher matcher = pattern.matcher(SubAddress);
            if (matcher.find()) {
                loclIP = matcher.group();
            }
        }
        ZMQ.Socket socket=context.socket(SocketType.PUSH);
        int port= socket.bindToRandomPort("tcp://"+loclIP);
        PushSocket pushSocket=new PushSocket(socket);
        pushSocket.port=port;
        pushSocket.ip=loclIP;
        pushSocket.topic=topic;
        pushSocket.identy =indey;

        return  pushSocket;

    }


    /**
     * 清理数据库日志，放在业务上处理
     */
    public void  clearDBLog()
    {
        if(bdbOperatorUtil!=null)
        {
            bdbOperatorUtil.clearLog();
        }
    }

    /**
     * 保留最近一段数据 毫秒
     * @param mis
     */
    public void delete(long mis)
    {
        if(bdbOperatorUtil!=null)
        {
            bdbOperatorUtil.delete(mis);
        }
    }
}
