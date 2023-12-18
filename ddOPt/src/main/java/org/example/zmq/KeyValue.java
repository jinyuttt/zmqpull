package org.example.zmq;

import java.io.Serializable;

/**
 * 查询数据
 * @param <T>
 */
public class KeyValue<T> implements Serializable {
    public  String key;

    public T data;

    public KeyValue(String keyString, T value) {
      this.key=keyString;
      this.data=value;
    }
}
