package com.github.mmolimar.kafka.connect.fs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TaskHandler extends Thread {

    private static final Logger log = LoggerFactory.getLogger(TaskHandler.class);

    private FsSourceTask parentTask;
    private long sleepDurationBeforeStop = 5;

    public TaskHandler(FsSourceTask parentTask, long sleepDurationBeforeStop) {
        this.parentTask = parentTask;
        this.sleepDurationBeforeStop = sleepDurationBeforeStop;
    }

    @Override
    public void run()
    {
        synchronized(parentTask) {
            long sleepDuration = sleepDurationBeforeStop * 60 * 1000;
            try {
                Thread.sleep(sleepDuration);
                log.info("Stop task");
                parentTask.stop();
            } catch (InterruptedException ie) {
                log.warn("An interrupted exception has occurred.", ie);
            }
        }
    }
}
