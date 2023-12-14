package org.example.zmq;

import com.sleepycat.je.*;
import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.rep.ReplicationConfig;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentSkipListSet;

/**
 * 集群数据库
 */
public class BDBReplicatedUtil implements IBDBUtil {


    /**
     * 集群交互地址
     */
    public  String peer="127.0.0.1:8946";

    /**
     * 本节点地址
     */
    public  String loalAddress="127.0.0.1:8946";

    /**
     * 群名称
     */
    public  String  cluster="mydd";
    private String dbEnvFilePath;
    private String databaseName;

    private Database utildb = null;

    private ReplicatedEnvironment replicatedEnvironment;

    private ConcurrentSkipListSet<String> set=new ConcurrentSkipListSet<>();
    public BDBReplicatedUtil(String dbEnvFilePath, String databaseName) {
        this.dbEnvFilePath = dbEnvFilePath;
        this.databaseName = databaseName;
        /**
         * 初始化数据库参数
         */
        try {
            File file = new File(dbEnvFilePath);
           if(!file.exists())
           {
               file.mkdirs();
           }


        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    /**
     * 初始化
     * @return
     */
    public boolean init() {
        try {
            File file = new File(dbEnvFilePath);
            String[] port = loalAddress.split(":");
            EnvironmentConfig envConfig = new EnvironmentConfig();
            envConfig.setAllowCreate(true);
            envConfig.setTransactional(true);
            envConfig.setNodeName(port[1]);
             envConfig.setDurability(Durability.COMMIT_WRITE_NO_SYNC);

            DatabaseConfig dbConfig = new DatabaseConfig();
            dbConfig.setReplicated(true);
            dbConfig.setTransactional(true);
            dbConfig.setAllowCreate(true);
            dbConfig.setSortedDuplicates(true);



            ReplicationConfig replicationConfig = new ReplicationConfig();
            replicationConfig.setNodeHostPort(loalAddress);
            replicationConfig.setGroupName(cluster);
            replicationConfig.setNodeName(port[1]);
            replicationConfig.setHelperHosts(peer);


            //  myEnvironment = new Environment(file, envConfig);
            replicatedEnvironment = new ReplicatedEnvironment(file, replicationConfig, envConfig);
            utildb = replicatedEnvironment.openDatabase(null, databaseName, dbConfig);
            return  true;
        }
        catch (Exception ex)
        {
            System.out.println(ex);
            return  false;
        }

    }


    /**
     * 读取字符串
     * @param key
     * @param value
     * @param isSync
     * @return
     */
    public boolean put(String key, String value, boolean isSync) {
        try {
            set.add(key);
            DatabaseEntry theKey = new DatabaseEntry(key.getBytes("UTF-8"));

            byte[]bytes=value.getBytes("UTF-8");
            ByteBuffer buffer= ByteBuffer.allocate(bytes.length+8);
            buffer.put(bytes);
            buffer.putLong(System.currentTimeMillis());
            DatabaseEntry theValue = new DatabaseEntry(buffer.array());
            utildb.put(null, theKey, theValue);
            if (isSync) {
                this.sync();
            }
            return true;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    /**
     * 存储字符串
     * @param key
     * @param value
     * @param isSync
     * @return
     */
    public boolean put(String key, byte[] value, boolean isSync) {
        try {
            set.add(key);
            DatabaseEntry theKey = new DatabaseEntry(key.getBytes("UTF-8"));
            ByteBuffer buffer= ByteBuffer.allocate(value.length+8);
            buffer.put(value);
            buffer.putLong(System.currentTimeMillis());
            DatabaseEntry theValue = new DatabaseEntry(buffer.array());

            utildb.put(null, theKey, theValue);
            if (isSync) {
                this.sync();
            }
            return true;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    /**
     * @param key
     *            删除bdb中指定的key值
     */
    public boolean delete(String key) {
        try {
            DatabaseEntry theKey = new DatabaseEntry(key.getBytes("UTF-8"));
            utildb.delete(null, theKey);
            set.remove(key);
            return true;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    /**
     * 移除保留时间以为的数据
     * @param key  关键字
     * @param mis 保留时间
     * @return
     */
    public int delete(String key,long mis) {

        Cursor  cursor=null;
        Transaction txn=null;
        int num=0;
        try
        {
            long cur=System.currentTimeMillis();
            TransactionConfig  transactionConfig=new TransactionConfig();
            transactionConfig.setReadUncommitted(true);
            transactionConfig.setSerializableIsolation(true);
            txn=replicatedEnvironment.beginTransaction(null,transactionConfig);
            CursorConfig cursorConfig=new CursorConfig();
            cursor=utildb.openCursor(txn,cursorConfig);
            DatabaseEntry theKey = new DatabaseEntry(key.getBytes("UTF-8"));
            DatabaseEntry theValue = new DatabaseEntry();
            while (cursor.getNext(theKey, theValue, LockMode.DEFAULT) ==
                    OperationStatus.SUCCESS ) {
                String keyString = new String(theKey.getData(), "UTF-8");
               ByteBuffer buffer=ByteBuffer.wrap(theValue.getData());
               long dt=buffer.getLong(theValue.getData().length-8);
              if(cur-dt>mis)
              {
                  cursor.delete();
                  num++;
              }
            }
            if (cursor!=null)
            {
                cursor.close();
            }
            if(txn!=null)
            {
                txn.commit();
            }
        } catch (Exception de) {
            System.err.println("Error accessing database." + de);
            txn.abort();
        } finally {
            // 使用后必须关闭游标
           // cursor.close();
        }
        return num;
    }

    @Override
    public void delete(long mis) {
        Cursor  cursor=null;
        Transaction txn=null;
        try
        {
            long cur=System.currentTimeMillis();
            TransactionConfig  transactionConfig=new TransactionConfig();
            transactionConfig.setReadUncommitted(true);
            transactionConfig.setSerializableIsolation(true);
            txn=replicatedEnvironment.beginTransaction(null,transactionConfig);
            CursorConfig cursorConfig=new CursorConfig();
            cursor=utildb.openCursor(txn,cursorConfig);
            DatabaseEntry theKey = new DatabaseEntry();
            DatabaseEntry theValue = new DatabaseEntry();
            while (cursor.getNext(theKey, theValue, LockMode.DEFAULT) ==
                    OperationStatus.SUCCESS ) {
                String keyString = new String(theKey.getData(), "UTF-8");
                ByteBuffer buffer=ByteBuffer.wrap(theValue.getData());
                long dt=buffer.getLong(theValue.getData().length-8);
                if(cur-dt>mis)
                {
                    cursor.delete();

                }
            }
            if (cursor!=null)
            {
                cursor.close();
            }
            if(txn!=null)
            {
                txn.commit();
            }
        } catch (Exception de) {
            System.err.println("Error accessing database." + de);
            txn.abort();
        } finally {
            // 使用后必须关闭游标
            // cursor.close();
        }

    }

    /**
     * 清除
     * @return
     */
    public boolean clear() {
        try {
            Iterator<String> itr = set.iterator();
            while (itr.hasNext()) {
                String key=itr.next();
                DatabaseEntry theKey = new DatabaseEntry(key.getBytes("UTF-8"));
                utildb.delete(null,theKey);
            }
            set.clear();
            return true;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    @Override
    public void clearLog() {
        if(replicatedEnvironment!=null)
        {
            replicatedEnvironment.cleanLog();
            replicatedEnvironment.cleanLogFile();
        }
    }

    @Override
    public long DBSize() {
       File file=new File(dbEnvFilePath);
        return FileUtils.sizeOfDirectory(file);

    }


    /**
     * 获取单值
     * @param key
     * @return
     */
    public String getValue(String key) {
        try {
            DatabaseEntry theKey = new DatabaseEntry(key.getBytes("UTF-8"));
            DatabaseEntry theValue = new DatabaseEntry();

            utildb.get(null, theKey, theValue, LockMode.DEFAULT);
            if (theValue.getData() == null) {
                return null;
            }

            return new String(theValue.getData(), "UTF-8");
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public List<String> getValues(String key) {
        List<String> list=new ArrayList<>();
        Cursor  cursor=null;
        try
        {
            CursorConfig cursorConfig=new CursorConfig();
            cursorConfig.setReadCommitted(true);
            cursor=utildb.openCursor(null,cursorConfig);
            DatabaseEntry theKey = new DatabaseEntry(key.getBytes("UTF-8"));
            DatabaseEntry theValue = new DatabaseEntry();
            while (cursor.getNext(theKey, theValue, LockMode.DEFAULT) ==
                    OperationStatus.SUCCESS ) {
                String keyString = new String(theKey.getData(), "UTF-8");
                list.add(new String(theValue.getData(),"UTF-8"));

            }
            return list;
        } catch (Exception de) {
            System.err.println("Error accessing database." + de);
        } finally {
            // 使用后必须关闭游标
            cursor.close();
        }
        return list;
    }


    @Override
    public byte[] getData(String key) {
        try {
            DatabaseEntry theKey = new DatabaseEntry(key.getBytes("UTF-8"));
            DatabaseEntry theValue = new DatabaseEntry();

            utildb.get(null, theKey, theValue, LockMode.DEFAULT);
            if (theValue.getData() == null) {
                return null;
            }

            return theValue.getData();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public List<byte[]> getDatas(String key) {
        List<byte[]> list=new ArrayList<>();
        Cursor  cursor=null;
        try
        {
            CursorConfig cursorConfig=new CursorConfig();
            cursorConfig.setReadCommitted(true);
            cursor=utildb.openCursor(null,cursorConfig);
            DatabaseEntry theKey = new DatabaseEntry(key.getBytes("UTF-8"));
            DatabaseEntry theValue = new DatabaseEntry();
            while (cursor.getNext(theKey, theValue, LockMode.DEFAULT) ==
                    OperationStatus.SUCCESS ) {
                String keyString = new String(theKey.getData(), "UTF-8");
                list.add(theValue.getData());

            }
            return list;
        } catch (Exception de) {
            System.err.println("Error accessing database." + de);
        } finally {
            // 使用后必须关闭游标
            cursor.close();
        }
        return list;
    }

    /**
     * 同步数据到碰盘当中，相当于让数据操作实时持久化
     */
    public boolean sync() {
        if (utildb != null) {
            try {
                utildb.sync();
                return true;
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return false;
    }

    /**
     * @return boolean 关闭数据库
     */
    public boolean close() {
        try {
            // // 先关闭数据库
            if (utildb != null) {
                utildb.close();
            }
            // // 再关闭BDB系统环境变量

            if(replicatedEnvironment!=null)
            {
                replicatedEnvironment.sync();
                replicatedEnvironment.cleanLog();
                replicatedEnvironment.close();
            }
            return true;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;

    }

    /**
     * 测试
     */
    public static void main(String[] args) {
        // 数据库所在的存储文件夹
        String dbEnvFilePath = "bdb2";
        // 数据库名称
        String databaseName = "zmq";
        String key = "zx";
        String value = "hello";

        String key1 = "zx";
        String value2 = "tttt";

        // 初始化
        BDBReplicatedUtil bdbUtil = new BDBReplicatedUtil(dbEnvFilePath,
                databaseName);
        bdbUtil.init();
        // 增加数据
        bdbUtil.put(key, value, false);
        bdbUtil.put(key1, value2, false);
     bdbUtil.getValues(key);
        bdbUtil.getValues(key);
        // 删除数据
        bdbUtil.delete(key1);
        // bdbUtil.sync();
        System.out.println(bdbUtil.getValue(key));

    }

}
