package org.example.zmq;

import com.sleepycat.je.*;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentSkipListSet;

/**
 * 本地数据库
 */
public class BDBLocalUtility implements IBDBUtil{
    private String dbEnvFilePath;
    private String databaseName;
    Environment myEnvironment = null;
    private Database utildb = null;
    private ConcurrentSkipListSet<String> set=new ConcurrentSkipListSet<>();

    /**
     *
     * @param dbEnvFilePath 路径
     * @param databaseName 数据库名称
     */
    public  BDBLocalUtility(String dbEnvFilePath, String databaseName)
    {
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
    @Override
    public boolean init() {
        try {
            File file = new File(dbEnvFilePath);

            EnvironmentConfig envConfig = new EnvironmentConfig();
            envConfig.setAllowCreate(true);

            envConfig.setDurability(Durability.COMMIT_WRITE_NO_SYNC);

            DatabaseConfig dbConfig = new DatabaseConfig();


            dbConfig.setAllowCreate(true);
            dbConfig.setSortedDuplicates(true);
           dbConfig.setDeferredWrite(true);

            myEnvironment = new Environment(file, envConfig);

            utildb = myEnvironment.openDatabase(null, databaseName, dbConfig);
            return  true;
        }
        catch (Exception ex)
        {
            System.out.println(ex);
            return  false;
        }

    }

    @Override
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

    @Override
    public boolean put(String key, byte[] data, boolean isSync) {
        try {
            set.add(key);
            DatabaseEntry theKey = new DatabaseEntry(key.getBytes("UTF-8"));
            ByteBuffer buffer= ByteBuffer.allocate(data.length+8);
            buffer.put(data);
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

    @Override
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

    @Override
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

    @Override
    public int delete(String key, long mis) {
        Cursor  cursor=null;
        Transaction txn=null;
        int num=0;
        try
        {
            long cur=System.currentTimeMillis();
            TransactionConfig  transactionConfig=new TransactionConfig();
            transactionConfig.setReadUncommitted(true);
            transactionConfig.setSerializableIsolation(true);
            txn=myEnvironment.beginTransaction(null,transactionConfig);
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
            txn=myEnvironment.beginTransaction(null,transactionConfig);
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

    @Override
    public boolean close() {
        try {
            // // 先关闭数据库
            if (utildb != null) {
                utildb.close();
            }
            // // 再关闭BDB系统环境变量
            if (myEnvironment != null) {
                myEnvironment.sync();
                myEnvironment.cleanLog(); // 在关闭环境前清理下日志
                myEnvironment.close();
            }

            return true;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    @Override
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
        if(myEnvironment!=null)
        {
            myEnvironment.cleanLog();
            myEnvironment.cleanLogFile();
        }
    }

    @Override
    public long DBSize() {
         File file=new File(dbEnvFilePath);
        return file.length();
    }

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

}
