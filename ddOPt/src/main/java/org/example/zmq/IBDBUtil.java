package org.example.zmq;

import java.util.List;


/**
 * 数据库Berkeley DB操作接口
 */
public interface IBDBUtil {

    /**
     * 初始化
     * @return
     */
    public  boolean init();

    /**
     * 保存
     * @param key
     * @param value
     * @param isSync
     * @return
     */
    public  boolean  put(String key,String value,boolean isSync);

    /**
     * 保存
     * @param key
     * @param data
     * @param isSync
     * @return
     */
    public  boolean  put(String key,byte[]data,boolean isSync);

    /**
     * 获取字符串
     * @param key
     * @return
     */
    public  String getValue(String key);

    /**
     * 获取字符串
     * @param key
     * @return
     */
    public List<String> getValues(String key);

    /**
     * 获取byte
     * @param key
     * @return
     */
    byte[] getData(String key);

    List<byte[]> getDatas(String key);

    /**
     * 删除
     * @param key
     * @return
     */
    public boolean  delete(String key);


    /**
     * 删除
     * @param key
     * @param mis
     * @return
     */
    public int  delete(String key,long mis);

    /**
     * 所有key删除时间以外的数据
     * @param mis
     */
    void  delete(long mis);

    /**
     * 关闭
     * @return
     */
 boolean close();

    /**
     *清理所有数据
     * @return
     */
    boolean clear();

    /**
     * 清理日志
     */
    void clearLog();

    /**
     * 大小M
     * @return
     */
    long DBSize();

    /**
     * 从环境中移除数据库
     */
    void removeDB();

    /**
     * 清空数据库
     * @return
     */
    long truncateDB();
}
