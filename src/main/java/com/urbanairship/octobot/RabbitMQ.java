package com.urbanairship.octobot;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import java.io.IOException;
import org.apache.log4j.Logger;

// This class handles all interfacing with AMQP / RabbitMQ in Octobot.
// It provides basic connection management and returns task channels
// for placing messages into a remote queue.
/**
 * 
 * @author 
 */
public class RabbitMQ
{

    private static final Logger logger = Logger.getLogger("RabbitMQ");
    public  static final ConnectionFactory factory = new ConnectionFactory();

    /**
     * 
     * @param queue
     */
    public RabbitMQ(Queue queue)
    {
        factory.setHost(queue.host);
        factory.setPort(queue.port);
        factory.setUsername(queue.username);
        factory.setPassword(queue.password);
        factory.setVirtualHost(queue.vhost);
    }

    // Returns a new connection to an AMQP queue.
    /**
     * 
     * @return
     * @throws IOException
     */
    public Connection getConnection() throws IOException
    {
        return factory.newConnection();
    }

    // Returns a live channel for publishing messages.
    /**
     * 
     * @return
     */
    public Channel getTaskChannel()
    {
        Channel taskChannel = null;

        int attempts = 0;
        while(true)
        {
            attempts++;
            logger.info("Attempting to connect to queue: attempt " + attempts);
            
            try
            {
                Connection connection = getConnection();
                taskChannel = connection.createChannel();
                break;
            }
            catch(IOException e)
            {
                logger.error("Error creating AMQP channel, retrying in 5 sec", e);
                try
                {
                    Thread.sleep(1000 * 5);
                }
                catch(InterruptedException ex)
                {
                }
            }
        }
        
        return taskChannel;
    }
}
