/*
 * Copyright © 2017 camunda services GmbH (info@camunda.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.zeebe.util.sched;

import java.time.Duration;
import java.util.concurrent.*;

import io.zeebe.util.sched.ZbActorScheduler.ActorSchedulerBuilder;
import io.zeebe.util.sched.metrics.TaskMetrics;
import org.agrona.concurrent.status.ConcurrentCountersManager;

/**
 * Used to submit {@link ActorTask ActorTasks} and Blocking Actions to
 * the scheduler's internal runners and queues.
 */
public class ActorExecutor
{
    private final ActorThreadGroup cpuBoundThreads;
    private final ActorThreadGroup ioBoundThreads;
    private final ThreadPoolExecutor blockingTasksRunner;
    private final ConcurrentCountersManager countersManager;
    private final Duration blockingTasksShutdownTime;

    public ActorExecutor(ActorSchedulerBuilder builder)
    {
        this.ioBoundThreads = builder.getIoBoundActorThreads();
        this.cpuBoundThreads = builder.getCpuBoundActorThreads();
        this.blockingTasksRunner = builder.getBlockingTasksRunner();
        this.countersManager = builder.getCountersManager();
        this.blockingTasksShutdownTime = builder.getBlockingTasksShutdownTime();
    }

    /**
     * Initially submit a non-blocking actor to be managed by this schedueler.
     *
     * @param task
     *            the task to submit
     * @param collectTaskMetrics
     *            Controls whether metrics should be collected. (See
     *            {@link ZbActorScheduler#submitActor(ZbActor, boolean)})
     */
    public void submitCpuBound(ActorTask task, boolean collectTaskMetrics)
    {
        submitTask(task, collectTaskMetrics, cpuBoundThreads);
    }

    public void submitIoBoundTask(ActorTask task, boolean collectTaskMetrics)
    {
        submitTask(task, collectTaskMetrics, ioBoundThreads);
    }

    private void submitTask(ActorTask task, boolean collectMetrics, ActorThreadGroup threadGroup)
    {
        TaskMetrics taskMetrics = null;

        if (collectMetrics)
        {
            taskMetrics = new TaskMetrics(task.getName(), countersManager);
        }

        task.onTaskScheduled(this, threadGroup, taskMetrics);

        threadGroup.submit(task);
    }

    /**
     * Sumbit a blocking action to run using the scheduler's blocking thread
     * pool
     *
     * @param action
     *            the action to submit
     */
    public void submitBlocking(Runnable action)
    {
        blockingTasksRunner.execute(action);
    }

    public void start()
    {
        cpuBoundThreads.start();
        ioBoundThreads.start();
    }

    public CompletableFuture<Void> closeAsync()
    {
        blockingTasksRunner.shutdown();

        final CompletableFuture<Void> resultFuture = CompletableFuture.allOf(ioBoundThreads.closeAsync(), cpuBoundThreads.closeAsync());

        try
        {
            blockingTasksRunner.awaitTermination(blockingTasksShutdownTime.getSeconds(), TimeUnit.SECONDS);
        }
        catch (InterruptedException e)
        {
            e.printStackTrace();
        }

        return resultFuture;
    }

    public ConcurrentCountersManager getCountersManager()
    {
        return countersManager;
    }
}
