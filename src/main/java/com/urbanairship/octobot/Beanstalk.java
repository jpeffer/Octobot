package com.urbanairship.octobot;

//mport com.surftools.BeanstalkClientImpl.ClientImpl;
import com.surftools.BeanstalkClientImpl.ClientImpl;
import org.apache.log4j.Logger;

/**
 * This class handles all interfacing with a Beanstalk in Octobot.
 * It is responsible for connection initialization and management.
 *
 * @author
 */
public class Beanstalk
{

    private static final Logger logger = Logger.getLogger("Beanstalk");
    private static final String ATTEMPT_NUMBER_PREFIX = "Attempt #";

    /**
     * Obtain the Beanstalk channel
     *
     * @param host
     * @param port
     * @param tube
     *
     * @return the obtained Beanstalk Channel
     */
    public static ClientImpl getBeanstalkChannel(String host, Integer port, String tube)
    {
        int attempts = 0;
        ClientImpl client = null;
        logger.info("Opening connection to Beanstalk tube: '" + tube + "'...");

        while(true)
        {
            attempts++;
            logger.debug(ATTEMPT_NUMBER_PREFIX);
            logger.debug(attempts);
            
            try
            {
                client = new ClientImpl(host, port);
                client.useTube(tube);
                client.watch(tube);
                logger.info("Connected to Beanstalk");
                break;
            }
            catch(Exception e)
            {
                logger.error("Unable to connect to Beanstalk. Retrying in 5 seconds", e);
                try
                {
                    Thread.sleep(1000 * 5);
                }
                catch(InterruptedException ex)
                {
                }                
            }
        }
        
        return client;
    }
}
