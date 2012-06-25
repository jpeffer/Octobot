package com.urbanairship.octobot;

import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.core.Timer;
import java.util.concurrent.TimeUnit;

/**
 * 
 * @author
 */
public class Metrics
{

    /**
     * 
     */
    protected static final MetricsRegistry registry = new MetricsRegistry();
    private static final String TIMER   = ":Timer";
    private static final String RETRIES = ":Retries";
    private static final String SUCCESS = ":Success";
    private static final String FAILURE = ":Failure";

    /**
     * Updates internal metrics following task execution.
     * 
     * @param task
     * @param time
     * @param status
     * @param retries
     */
    public static void update(String task, long time, boolean status, int retries)
    {
        updateExecutionTimes(task, time);
        updateTaskRetries(task, retries);
        updateTaskResults(task, status);
    }

    /**
     * Update the list of execution times, keeping the last 10,000 per task.
     * 
     * @param task
     * @param time 
     */
    private static void updateExecutionTimes(String task, long time)
    {
        MetricName timerName = new MetricName(OctobotConstants.OCTOBOT, OctobotConstants.METRICS, task + TIMER);
        Timer timer = registry.newTimer(timerName, TimeUnit.NANOSECONDS, TimeUnit.MILLISECONDS);
        timer.update(time, TimeUnit.NANOSECONDS);
    }

    /**
     * Update the number of times this task has been retried.
     * 
     * @param task
     * @param retries 
     */
    private static void updateTaskRetries(String task, int retries)
    {
        MetricName counterRetriesName = new MetricName(OctobotConstants.OCTOBOT, OctobotConstants.METRICS, task + RETRIES);
        Counter counterRetries = registry.newCounter(counterRetriesName);
        counterRetries.inc();
    }

    /**
     * Update the number of times this task has succeeded or failed.
     * 
     * @param task
     * @param status 
     */
    private static void updateTaskResults(String task, boolean status)
    {
        if(status == true)
        {
            MetricName counterSuccessName = new MetricName(OctobotConstants.OCTOBOT, OctobotConstants.METRICS, task + SUCCESS);
            Counter counterSuccess = registry.newCounter(counterSuccessName);
            counterSuccess.inc();
        }
        else
        {
            MetricName counterFailureName = new MetricName(OctobotConstants.OCTOBOT, OctobotConstants.METRICS, task + FAILURE);
            Counter counterFailure = registry.newCounter(counterFailureName);
            counterFailure.inc();
        }
    }
}
