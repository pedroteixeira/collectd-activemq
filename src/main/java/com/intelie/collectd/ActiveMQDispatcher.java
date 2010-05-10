package com.intelie.collectd;

import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.collectd.api.DataSource;
import org.collectd.api.Notification;
import org.collectd.api.PluginData;
import org.collectd.api.ValueList;
import org.collectd.protocol.Dispatcher;
import org.collectd.protocol.Network;
import org.collectd.protocol.TypesDB;
import org.collectd.protocol.UdpReceiver;

import javax.jms.*;
import java.text.Format;
import java.util.ArrayList;
import java.util.List;

/**
 * Dispatch collectd data to activemq.
 * java -classpath <collectd.jar,...> com.intelie.collectd.ActiveMQDispatcher
 */
public class ActiveMQDispatcher implements Dispatcher {

    private static final Log LOG = LogFactory.getLog(ActiveMQDispatcher.class);

    private final boolean namesOnly = "true".equals(Network.getProperty("namesOnly"));
    private String queue = "collectd";
    private String eventType = "collectd";
    private String brokerUrl = "failover:tcp://localhost:61616";
    private String delimiter = "/";

    private final MessageProducer producer;
    private final QueueSession session;


    protected void loadProperties() {
        brokerUrl = System.getProperty("activemq.url", brokerUrl);
        queue = System.getProperty("activemq.queue", queue);
        eventType = System.getProperty("eventType", eventType);
    }


    public ActiveMQDispatcher() throws JMSException {
        loadProperties();

        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(brokerUrl);
        QueueConnection conn = factory.createQueueConnection();
        session = conn.createQueueSession(false, javax.jms.Session.AUTO_ACKNOWLEDGE);
        producer = session.createProducer(new ActiveMQQueue(queue));
        producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
    }


    public void dispatch(ValueList vl) {

        String output = getOutput(vl);
        String json = getJson(vl.getHost(), vl.getTime(), vl, output);

        System.out.println(json);

        try {
            producer.send(session.createTextMessage(json));
        } catch (JMSException e) {
            LOG.error("Error sending values '" + vl.toString() + "'.", e);
        }
    }

    public void dispatch(Notification notification) {
        String output = " [" + notification.getSeverityString() + "] " + notification.getMessage();
        String json = getJson(notification.getHost(), notification.getTime(), notification, output);

        System.out.println(json);

        try {
            producer.send(session.createTextMessage(json));
        } catch (JMSException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }


    /**
     * Convention for collectd output.
     *
     * @return
     */
    protected String getOutput(ValueList vl) {

        StringBuffer sb = new StringBuffer();

        List<DataSource> ds = vl.getDataSource();
        List<Number> vals = vl.getValues();
        int size = vals.size();

        for (int i = 0; i < size; i++) {
            Number val = vals.get(i);
            String name = ds == null ? null : ds.get(i).getName();

            if (name != null && !name.isEmpty()) {
                sb.append(name).append(":");
            }
            sb.append(val);

            if (i < size - 1) sb.append(";");
        }

        return sb.toString();
    }

    protected String getJson(String host, Long ts, PluginData plugin, String output) {

        StringBuffer json = new StringBuffer("{");
        json.append("'eventtype':").append("'").append(eventType).append("'").append(",");
        json.append("'host':").append("'").append(host).append("'").append(",");

        appendJsonProperty(json, "plugin", plugin.getPlugin());
        json.append(",");
        appendJsonProperty(json, "pluginInstance", plugin.getPluginInstance());
        json.append(",");
        appendJsonProperty(json, "type", plugin.getType());
        json.append(",");
        appendJsonProperty(json, "typeInstance", plugin.getTypeInstance());
        json.append(",");
        json.append("'timestamp':").append(ts).append(",");
        json.append("'values':").append("'").append(cleanString(output)).append("'");
        json.append("}");
        return json.toString();
    }

    protected void appendJsonProperty(StringBuffer sb, String key, String value) {
        sb.append("'").append(key).append("':");
        if (value != null && !value.isEmpty()) {
            sb.append("'").append(value).append("'");
        } else {
            sb.append("null");
        }
    }

    protected String cleanString(String out) {
        return out.replaceAll("'", "\\'");
    }

    public static void main(String[] args) throws Exception {
        new UdpReceiver(new ActiveMQDispatcher()).listen();
    }


}
