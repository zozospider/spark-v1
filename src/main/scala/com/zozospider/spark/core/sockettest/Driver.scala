package com.zozospider.spark.core.sockettest

import java.io.{ObjectOutputStream, OutputStream}
import java.net.{ServerSocket, Socket}

// client 客户端
object Driver {

  def main(args: Array[String]): Unit = {

    val task: Task = new Task
    val subTask1: SubTask = new SubTask
    subTask1.data = task.data.take(2)
    subTask1.logic = task.logic
    val subTask2: SubTask = new SubTask
    subTask2.data = task.data.takeRight(2)
    subTask2.logic = task.logic

    // 连接服务器
    val client1: Socket = new Socket("localhost", 8888)
    val client2: Socket = new Socket("localhost", 9999)

    // 输出流, 对象输出流
    val out1: OutputStream = client1.getOutputStream
    val out2: OutputStream = client2.getOutputStream
    val objectOut1: ObjectOutputStream = new ObjectOutputStream(out1)
    val objectOut2: ObjectOutputStream = new ObjectOutputStream(out2)
    objectOut1.writeObject(subTask1)
    objectOut2.writeObject(subTask2)

    out1.flush()
    out2.flush()
    out1.close()
    out2.close()
    client1.close()
    client2.close()
    println("客户端数据发送完毕")
  }

}
