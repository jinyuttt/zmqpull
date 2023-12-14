package org.example.zmq;

 class InnerData {

    public  String topic;

    public  String client;

    public  byte[] data;

    public InnerData(String topic, String client, byte[] data) {
        this.client=client;
        this.topic=topic;
        this.data=data;
    }
}
