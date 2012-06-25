package com.urbanairship.octobot;

import com.yammer.metrics.reporting.MetricsServlet;
import org.apache.log4j.Logger;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

/**
 * 
 * @author
 */
public class Introspector implements Runnable
{

    private static final Logger logger = Logger.getLogger("Introspector");

    /**
     * 
     */
    @Override
    public void run()
    {
        /*
         * HealthChecks.register(new RedisHealthcheck)
         * HealthChecks.register(new RabbitHealthcheck)
         * HealthChecks.register(new BeanstalkHealthcheck)
         */

        int port = Settings.getAsInt(OctobotConstants.OCTOBOT, "metrics_port");
        if(port < 1)
        {
            port = 1228;
        }

        Server server = new Server(port);
        ServletHolder holder = new ServletHolder(MetricsServlet.class);
        ServletContextHandler context = new ServletContextHandler();

        context.setContextPath(OctobotConstants.EMPTY);
        context.addServlet(holder, OctobotConstants.FORWARD_SLASH_STAR);

        server.setHandler(context);

        logger.info("Introspector launching on port: " + port);
        try
        {
            server.start();
            server.join();
        }
        catch(Exception e)
        {
            logger.error("Introspector: Unable to listen on port: " + port
                    + ". Introspector will be unavailable on this instance.");
        }
    }
}
