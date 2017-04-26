package com.xiaobitipao.sample.activemq;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;

/**
 * 消息的消费者（接受者）
 */
public class JMSConsumer {

    // 默认连接用户名
    private static final String USERNAME = ActiveMQConnection.DEFAULT_USER;

    // 默认连接密码
    private static final String PASSWORD = ActiveMQConnection.DEFAULT_PASSWORD;

    // 默认连接地址 [tcp：//localhost:61616]
    private static final String BROKEURL = ActiveMQConnection.DEFAULT_BROKER_URL;

    public static void main(String[] args) {

        // 第一步：建立 ConnectionFactory 工厂对象:
        // 需要填入用户名，密码，以及要连接的地址，这里都使用默认值。其中默认地址为：[tcp：//localhost:61616]
        ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(USERNAME, PASSWORD, BROKEURL);

        // 由于 Connection 没有实现 java.io.Closeable 接口
        // 所以不能通过 try 语句简化 close 方法调用
        Connection connection = null;

        try {
            // 第二步 ：通过 ConnectionFactory 工厂对象创建一个 Connection 连接
            // 默认 Connection 是关闭的，需要调用 Connection 的 start 方法开启连接
            connection = connectionFactory.createConnection();
            connection.start();

            // 第三步：通过 Connection 对象创建 Session 会话(上下文环境对象)，用于接收或者发送消息:
            // 第一个参数为是否启用事务，第二个参数为签收模式，一般设置为自动签收。
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            // 第四步：通过 Session 创建 Destination 对象，该对象指定生产消息目标和消费消息来源。
            // 在 PTP 模式中，Destination 被称作 Queue 即队列，
            // 在 Pub/Sub 模式中，Destination 被称作 Topic 即主题,
            // 在程序中可以使用多个 Queue 和 Topic。
            Destination destination = session.createQueue("queue1");

            // 第五步：通过 Session 对象创建消息的发送和接收对象(生产者和消费者)
            // MessageProducer/MessageConsumer。
            MessageConsumer messageConsumer = session.createConsumer(destination);

            // 第六步：客户端使用 MessageConsumer 的 receive 方法接收数据
            recieveMessage(messageConsumer);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * 接收消息
     * 
     * @param messageConsumer
     *            消息消费者
     * @throws Exception
     */
    public static void recieveMessage(MessageConsumer messageConsumer) throws Exception {
        while (true) {
            TextMessage message = (TextMessage) messageConsumer.receive();
            if (message != null) {
                System.out.println("收到的消息:" + message.getText());
            } else {
                break;
            }
        }
    }
}
