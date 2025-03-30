package ru.dksu;

import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicInteger;

public class CollaborativeQueue<TaskT extends CollaborativeTask> {
    private ConcurrentLinkedDeque<CollaborativeTask> tasksQueue = new ConcurrentLinkedDeque<>();
    private AtomicInteger inProgress = new AtomicInteger(0);

    public void add(TaskT task) {
        tasksQueue.addLast(task);
    }

    // может вызывать только тот поток, который добавлял до этого таски
    public boolean isFinished() {
        return inProgress.get() == 0 && tasksQueue.isEmpty();
    }

    public void helpIfNeed() {
        if (tasksQueue.isEmpty()) {
            return;
        }
        inProgress.incrementAndGet();
        while (true) {
            var task = tasksQueue.pollFirst();
            if (task == null) {
                inProgress.decrementAndGet();
                return;
            }
//            System.out.println(Thread.currentThread().toString() + " executes task");
            task.start();
        }
    }
}
