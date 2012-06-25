package com.urbanairship.octobot;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import org.json.simple.JSONObject;

/**
 * 
 * @author
 */
public class TaskExecutor
{

    private static final HashMap<String, Method> taskCache =
                                                 new HashMap<String, Method>();

    /**
     * 
     * @param taskName
     * @param message
     * 
     * @throws ClassNotFoundException
     * @throws NoSuchMethodException
     * @throws IllegalAccessException
     * @throws InvocationTargetException
     */
    @SuppressWarnings("unchecked")
    public static void execute(String taskName, JSONObject message)
            throws ClassNotFoundException,
                   NoSuchMethodException,
                   IllegalAccessException,
                   InvocationTargetException
    {

        Method method = null;

        if(taskCache.containsKey(taskName))
        {
            method = taskCache.get(taskName);
        }
        else
        {
            Class task = Class.forName(taskName);

            /**
             * TODO: Replace with real caching solution as retaining every new
             *       task in memory will become problematic if dealing with a large
             *       set of unique tasks
             */
            method = task.getMethod(OctobotConstants.RUN, 
                                    new Class[]{JSONObject.class});            
            taskCache.put(taskName, method);
        }

        method.invoke(null, new Object[]{ message });
    }
}
