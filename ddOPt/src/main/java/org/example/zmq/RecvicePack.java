package org.example.zmq;

import java.util.concurrent.ConcurrentHashMap;

public class RecvicePack {
   private ConcurrentHashMap<String,Integer> mapRate=new ConcurrentHashMap<>();

    public void  add(String topic)
    {
      int num=  mapRate.getOrDefault(topic,0);
      mapRate.put(topic,num+1);
    }

    public  int getRate(String topic)
    {
        int num=  mapRate.getOrDefault(topic,0);
        mapRate.remove(topic);
        return  num;
    }
}
