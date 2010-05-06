package com.intelie.collectd;

import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.collectd.api.DataSource;
import org.collectd.api.Notification;
import org.collectd.api.ValueList;
import org.collectd.protocol.Dispatcher;
import org.collectd.protocol.Network;
import org.collectd.protocol.TypesDB;
import org.collectd.protocol.UdpReceiver;

import javax.jms.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Dispatch collectd data to activemq.
 * java -classpath <collectd.jar,...> com.intelie.collectd.ActiveMQDispatcher
 */
public class ActiveMQDispatcher implements Dispatcher {

    private static final Log LOG = LogFactory.getLog(ActiveMQDispatcher.class);
    private final boolean namesOnly = "true".equals(Network.getProperty("namesOnly"));

    private final MessageProducer producer;
    private final QueueSession session;

    public ActiveMQDispatcher() throws JMSException {
        LOG.info("Init");
        System.out.println("init");

        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory("failover:tcp://localhost:61616");
        QueueConnection conn = factory.createQueueConnection();
        session = conn.createQueueSession(false, javax.jms.Session.AUTO_ACKNOWLEDGE);
        producer = session.createProducer(new ActiveMQQueue("collectd"));
        producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

    }


    public void dispatch(ValueList vl) {
        if (namesOnly) {
            System.out.print("plugin=" + vl.getPlugin());
            System.out.print(",pluginInstance=" + vl.getPluginInstance());
            System.out.print(",type=" + vl.getType());
            System.out.print(",typeInstance=" + vl.getTypeInstance());
            List<DataSource> ds = vl.getDataSource();
            if (ds == null) {
                ds = TypesDB.getInstance().getType(vl.getType());
            }
            if (ds != null) {
                List<String> names = new ArrayList<String>();
                for (int i = 0; i < ds.size(); i++) {
                    names.add(ds.get(i).getName());
                }
                System.out.print("-->" + names);
            }
            System.out.println();
        } else {
            System.out.println(vl);
        }

        try {
            producer.send(session.createTextMessage(vl.toString()));
        } catch (JMSException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }

    public void dispatch(Notification notification) {
        System.out.println(notification);

        try {
            producer.send(session.createTextMessage(notification.toString()));
        } catch (JMSException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }


    public static void main(String[] args) throws Exception {
        new UdpReceiver(new ActiveMQDispatcher()).listen();
    }


}
