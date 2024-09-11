package com.opentext.assignment;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.IntStream;

import static java.lang.Thread.currentThread;
import static java.util.UUID.randomUUID;
import static java.util.concurrent.CompletableFuture.supplyAsync;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class Main {

    private static final String TASK_SUBMITTED_MSG = "++++ Task submitted with TaskUUID: %s & TaskGroupUUID: %s by: %s";
    private static final String TASK_COMPLETED_MSG = "---> Task completed with TaskUUID: %s & TaskGroupUUID: %s by: %s";

    /**
     * This method is added to test the functionality of TaskExecutorService.
     *
     * @param args the input arguments
     */
    public static void main(String[] args) {
        int totalTasks = 10;
        int currency = 10;
        TaskExecutorService taskExecutor = new Main().new TaskExecutorService(currency);
        System.out.println("Total tasks to execute: " + totalTasks);
        List<Future<?>> futureList = new ArrayList<>();
        UUID grp_1 = randomUUID();
        System.out.println("TaskGroupUUID : " + grp_1 + " is used to specify tasks with same TaskGroup.\n");

        // Logic to submit tasks to the TaskExecutor
        IntStream.range(0, totalTasks).forEach(n -> futureList.add(taskExecutor.submitTask(
                getTask(randomUUID(), getRandomGroup(grp_1, randomUUID()), getRandomTaskType()))));

        // Printing result of async task execution here....
        for (Future<?> task : futureList) {
            if (task instanceof CompletableFuture t) {
                t.thenAccept(System.out::println);
            }
        }

        taskExecutor.shutdown();
    }

    private static UUID getRandomGroup(UUID groupOne, UUID groupTwo) {

        return new UUID[]{groupOne, groupTwo}[(new Random().nextInt(0, 2))];
    }

    private static TaskType getRandomTaskType() {

        return TaskType.values()[(new Random().nextInt(0, 2))];
    }

    /**
     * This method os added to create Async Tasks for concurrent execution.
     *
     * @param taskUUID      accepts the taskUUID of a Task
     * @param taskGroupUUID accepts the taskGroupUUID of a Task
     * @param taskType      accepts the taskType of a Task
     * @return Task<String> task returns the created Task
     */
    private static Task<String> getTask(UUID taskUUID, UUID taskGroupUUID, TaskType taskType) {
        System.out.println(getMessageToPrint(TASK_SUBMITTED_MSG, taskUUID, taskGroupUUID));

        return new Task<>(taskUUID, new TaskGroup(taskGroupUUID), taskType, () ->
                getMessageToPrint(TASK_COMPLETED_MSG, taskUUID, taskGroupUUID));
    }

    /**
     * This method is added to create task submission/completion message.
     *
     * @param taskUUID
     * @param taskGroupUUID
     * @return String returns task submission/completion message to the caller.
     */
    private static String getMessageToPrint(String msg, UUID taskUUID, UUID taskGroupUUID) {

        return msg.formatted(taskUUID, taskGroupUUID, currentThread().getName());
    }

    /**
     * Enumeration of task types.
     */
    public enum TaskType {
        READ,
        WRITE,
    }

    public interface TaskExecutor {
        /**
         * Submit new task to be queued and executed.
         *
         * @param task Task to be executed by the executor. Must not be null.
         * @return Future for the task asynchronous computation result.
         */
        <T> Future<T> submitTask(Task<T> task);
    }

    /**
     * This class implements TaskExecutor, to provide all the functionality required as per the given problem statement.
     */
    private class TaskExecutorService implements TaskExecutor {
        private final ExecutorService executor;
        /**
         * This variable stores the Task & associated GroupLock.
         */
        private final ConcurrentHashMap<UUID, ReentrantLock> taskGroupLockerHolder;
        /**
         * This lock is useful in-order to preserve the submission order during the execution of async tasks
         */
        private final Semaphore concurrentTaskExecutor;

        /**
         * 1. Added to initialize ExecutorService with required concurrent threads to execute the submitted task.
         * 2. It initializes concurrentTaskExecutor, which is helpful in preserving execution order.
         * 3. It also initializes 'taskGroupLockerHolder', which holds the TaskGroup Lock needed to ensure that
         * Tasks sharing the same TaskGroup must not run concurrently.
         *
         * @param countOfAsyncTasksToExecute the count of async tasks to execute
         */
        private TaskExecutorService(int countOfAsyncTasksToExecute) {
            this.executor = newFixedThreadPool(countOfAsyncTasksToExecute);
            this.concurrentTaskExecutor = new Semaphore(countOfAsyncTasksToExecute, true);
            this.taskGroupLockerHolder = new ConcurrentHashMap<>();
        }

        /**
         * This method is added to perform the following actions:
         * 1. Creates the 'taskGroupLockerHolder' to maintain the associated taskGroupLock, needed to prevent the
         * concurrent execution of Tasks, sharing the same the TaskGroup.
         * 2. Submits the task concurrently without blocking the submitter.
         * 3. Ensures that Tasks sharing same TaskGroupUUID does not execute concurrently.
         * 4. Maintains the orders at the time of Task execution.
         *
         * @param task Task to be executed by the executor. Must not be null.
         * @return Future<T> Returns the result of execution of submitted task.
         */
        @Override
        public <T> Future<T> submitTask(Task<T> task) {

            return supplyAsync(() -> {
                UUID groupId = task.taskGroup().groupUUID();
                taskGroupLockerHolder.put(groupId, taskGroupLockerHolder.getOrDefault(groupId, new ReentrantLock(true)));
                try {
                    // This lock is to preserve the task submission order, during the task execution.
                    concurrentTaskExecutor.acquire();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                // This lock is to prevent the concurrent execution of Tasks sharing the same TaskGroupUUID.
                ReentrantLock groupLock = taskGroupLockerHolder.get(groupId);
                groupLock.lock();
                try {
                    return task.taskAction.call();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    groupLock.unlock();
                    concurrentTaskExecutor.release();
                }
            }, executor);
        }

        /**
         * This method added to attempt to shut down the executor service, if no thread is under execution and awaits
         * for the 60 secs for running thread to complete before shutdown.
         */
        private void shutdown() {
            executor.shutdown();
            try {
                if (!executor.awaitTermination(1000, MILLISECONDS)) {
                    executor.shutdownNow();
                }
            } catch (InterruptedException e) {
                executor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * Representation of computation to be performed by the {@link TaskExecutor}.
     *
     * @param taskUUID   Unique task identifier.
     * @param taskGroup  Task group.
     * @param taskType   Task type.
     * @param taskAction Callable representing task computation and returning the result.
     * @param <T>        Task computation result value type.
     */
    public record Task<T>(
            UUID taskUUID,
            TaskGroup taskGroup,
            TaskType taskType,
            Callable<T> taskAction
    ) {
        public Task {
            if (taskUUID == null || taskGroup == null || taskType == null || taskAction == null) {
                throw new IllegalArgumentException("All parameters must not be null");
            }
        }
    }

    /**
     * Task group.
     *
     * @param groupUUID Unique group identifier.
     */
    public record TaskGroup(
            UUID groupUUID
    ) {
        public TaskGroup {
            if (groupUUID == null) {
                throw new IllegalArgumentException("All parameters must not be null");
            }
        }
    }
}
