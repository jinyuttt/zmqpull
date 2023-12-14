package org.example;

import Zmq.protobuf.ZmqProbuf;
import com.google.protobuf.ByteString;

import java.io.IOException;

public class DemoProto {

    public  byte[]  test()
    {
        ZmqProbuf.ZmqMessage.Builder builder=ZmqProbuf.ZmqMessage.newBuilder();
        builder.setId(1111);
        builder.setMsgType(ZmqProbuf.MessageType.Str);
        builder.setMsg("sssssss");
        builder.setBytesmsg(ByteString.copyFromUtf8("ddddd"));
        ZmqProbuf.ZmqMessage msg = builder.build();
        byte[] buf = msg.toByteArray();
        return  buf;
    }

    public void  ss(byte[]bytes) throws IOException {

        ZmqProbuf.ZmqMessage person2 = ZmqProbuf.ZmqMessage.parseFrom(bytes);
    }

    public static void main(String[] args) {
        DemoProto proto=new DemoProto();
        try {
            proto.ss(proto.test());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

}
