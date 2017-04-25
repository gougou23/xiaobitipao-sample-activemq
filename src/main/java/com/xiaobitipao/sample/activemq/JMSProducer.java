package com.xiaobitipao.sample.activemq;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;

/**
 * 消息的生产者（发送者）
 */
public class JMSProducer {

    // 默认连接用户名
    private static final String USERNAME = ActiveMQConnection.DEFAULT_USER;

    // 默认连接密码
    private static final String PASSWORD = ActiveMQConnection.DEFAULT_PASSWORD;

    // 默认连接地址(tcp：//localhost:61616)
    private static final String BROKEURL = ActiveMQConnection.DEFAULT_BROKER_URL;
    // private static final String BROKEURL = "tcp：//localhost:61616";

    // 发送的消息数量
    private static final int SENDNUM = 10;

    public static void main(String[] args) {

        System.out.println(BROKEURL);
        // 第一步：建立 ConnectionFactory 工厂对象:
        // 需要填入用户名，密码，以及要连接的地址，这里都使用默认值。其中默认地址为：[tcp：//localhost:61616]
        ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(USERNAME, PASSWORD, BROKEURL);

        Connection connection = null;

        try {
            // 第二步 ：通过 ConnectionFactory 工厂对象创建一个 Connection 连接
            // 默认 Connection 是关闭的，需要调用 Connection 的 start 方法开启连接
            connection = connectionFactory.createConnection();
            connection.start();

            // 第三步：通过 Connection 对象创建 Session 会话(上下文环境对象)，用于接收或者发送消息:
            // 第一个参数为是否启用事务，第二个参数为签收模式，一般设置为自动签收。
            Session session = connection.createSession(true, Session.AUTO_ACKNOWLEDGE);

            // 第四步：通过 Session 创建 Destination 对象，该对象指定生产消息目标和消费消息来源。
            // 在 PTP 模式中，Destination 被称作 Queue 即队列，
            // 在 Pub/Sub 模式中，Destination 被称作 Topic 即主题,
            // 在程序中可以使用多个 Queue 和 Topic。
            Destination destination = session.createQueue("queue1");

            // 第五步：通过 Session 对象创建消息的发送和接收对象(生产者和消费者)
            // MessageProducer/MessageConsumer。
            MessageProducer messageProducer = session.createProducer(destination);

            // 第六步：使用 MessageProducer 的 setDeliveryMode
            // 方法为其设置持久化特性和非持久化特性(DeliveryMode)
            messageProducer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

            // 第七步：使用 JMS 规范的 TextMessage 形式通过 Session 对象创建数据，
            // 并用 MessageProducer 的 send 方法发送数据,客户端使用 recieve 方法接收数据。
            sendMessage(session, messageProducer);
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
     * 发送消息
     * 
     * @param session
     * @param messageProducer
     *            消息生产者
     * @throws Exception
     */
    public static void sendMessage(Session session, MessageProducer messageProducer) throws Exception {
        for (int i = 0; i < SENDNUM; ++i) {
            // 创建一条文本消息，通过消息生产者发送
            TextMessage message = session.createTextMessage();
            message.setText("ActiveMQ 发送消息, id为 " + i);
            messageProducer.send(message);
            System.out.println("发送消息：" + message.getText());
        }
    }
}