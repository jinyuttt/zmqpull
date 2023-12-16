package org.example.zmq;

/**
 * 数据库操作
 */
public enum DBOpt {
    AddDataDB,//添加
    DeleteDataDB,//删除
    DeleteDataDBMis,//删除
    ClearDataDB,//清除
    QueryDataDB,//查询

    AckQueryData;//返回数据库查询结果
}
