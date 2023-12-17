package HessianObj;


/**
 * 二进制序列化
 */
public interface SerializeFactory {

    /**
     * 序列化
     * @param t
     * @return
     * @param <T>
     */
    public <T> byte[] serialize(T t);


    /**
     * 反序列化
     * @param data
     * @param clazz
     * @return
     * @param <T>
     */
    public <T> T deserialize(byte[] data, Class<T> clazz);
}
