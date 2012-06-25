package com.urbanairship.octobot;

import java.util.HashMap;

/**
 * 
 * @author
 */
public class Queue
{

    public String queueType;
    public String queueName;

    public String host;
    public Integer port;
    public String username;
    public String password;
    public String vhost;

    /**
     * 
     * @param queueType
     * @param queueName
     * @param host
     * @param port
     * @param username
     * @param password
     */
    public Queue(String queueType, String queueName, String host, Integer port,
                 String username, String password)
    {
        this.queueType  = queueType.toLowerCase();
        this.queueName  = queueName;
        this.host       = host;
        this.port       = port;
        this.username   = username;
        this.password   = password;
        this.vhost      = OctobotConstants.FORWARD_SLASH;
    }

    /**
     * 
     * @param queueType
     * @param queueName
     * @param host
     * @param port
     */
    public Queue(String queueType, String queueName, String host, Integer port)
    {
        this.queueType  = queueType.toLowerCase();
        this.queueName  = queueName;
        this.host       = host;
        this.port       = port;
    }

    /**
     * 
     * @param config
     */
    public Queue(HashMap<String, Object> config)
    {
        this.queueName  = (String) config.get(OctobotConstants.QUEUE_NAME);
        this.queueType  = config.get(OctobotConstants.QUEUE_PROTOCOL) == null ? OctobotConstants.EMPTY: ((String)config.get(OctobotConstants.QUEUE_PROTOCOL)).toLowerCase();
        this.host       = (String) config.get(OctobotConstants.QUEUE_HOST);
        this.vhost      = (String) config.get(OctobotConstants.QUEUE_VHOST);
        this.username   = (String) config.get(OctobotConstants.QUEUE_USERNAME);
        this.password   = (String) config.get(OctobotConstants.QUEUE_PASSWORD);

        if(config.get(OctobotConstants.QUEUE_PORT) != null)
        {
            this.port = Integer.parseInt(((Long) config.get(OctobotConstants.QUEUE_PORT)).toString());
        }
    }

    /**
     * 
     * @return
     */
    @Override
    public String toString()
    {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(queueType);
        stringBuilder.append(OctobotConstants.FORWARD_SLASH);
        stringBuilder.append(queueName);
        stringBuilder.append(OctobotConstants.FORWARD_SLASH);
        stringBuilder.append(host);
        stringBuilder.append(OctobotConstants.FORWARD_SLASH);
        stringBuilder.append(port);
        stringBuilder.append(OctobotConstants.FORWARD_SLASH);
        stringBuilder.append(username);
        stringBuilder.append(OctobotConstants.FORWARD_SLASH);
        stringBuilder.append(password);
        stringBuilder.append(OctobotConstants.FORWARD_SLASH);
        stringBuilder.append(vhost);
                      
        return stringBuilder.toString();
    }
}
