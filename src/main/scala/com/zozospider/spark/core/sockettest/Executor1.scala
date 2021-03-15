package com.zozospider.spark.core.sockettest

import java.io.{InputStream, ObjectInputStream}
import java.net.{ServerSocket, Socket}

// server 服务端
object Executor1 {

  def main(args: Array[String]): Unit = {

    // 启动服务器接收数据
    val server: ServerSocket = new ServerSocket(8888)

    // 等待客户端连接
    println("服务器[8888] 启动等待接收数据")
    val client: Socket = server.accept
    // 输入流
    val input: InputStream = client.getInputStream
    // val i: Int = input.read
    // 对象输入流
    val objectInput: ObjectInputStream = new ObjectInputStream(input)
    val subTask: SubTask = objectInput.readObject().asInstanceOf[SubTask]
    println(s"接收到客户端发送的数据: subTask = $subTask")
    val result: Seq[Int] = subTask.compute()
    println(s"计算节点[8888] 计算的结果为: result = $result")

    input.close()
    client.close()
    server.close()
  }

}
