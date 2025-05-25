package ru.dksu;

import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Основной класс коллаборативной очереди.
 * <p>
 * Предоставляет возможность добавления задач в очередь, а также подключения
 * потоков к коллаборативной очереди.
 * <p>
 * Содержит также методы, помогающие подключить коллаборативную помощь в наиболее
 * частых ситуациях.
 *
 * @param <TaskT> тип-параметр для возможности ограничения типов задач, с которыми работает очередь.
 *
 * @author DenisKorotchenko
 */
public class CollaborativeQueue<TaskT extends CollaborativeTask> {
    private final ConcurrentLinkedDeque<CollaborativeTask> tasksQueue = new ConcurrentLinkedDeque<>();
    private final AtomicInteger inProgress = new AtomicInteger(0);
    private final AtomicBoolean canPoll = new AtomicBoolean(true);

    /**
     * Метод для добавления новых задач в коллаборативную очередь.
     *
     * Потокобезопасен, может вызываться из нескольких потоков, однако
     * основной сценарий использования предполагает вызов этого метода
     * из потока, инициировавшего блокирующую (коллаборативную) операцию.
     *
     * После вызова метода задача немедленно станет доступна для выполнения потоками,
     * выполняющими коллаборативную помощь, в случае если взятие задач из очереди включено.
     * @param task задача, которую необходимо добавить в очередь.
     */
    public void add(TaskT task) {
        tasksQueue.addLast(task);
    }

    /**
     * Метод, отключающий взятия задач из очереди.
     * <p>
     * Должен быть вызван до добавления первой задачи в случае, если
     * нельзя начать выполнение задач до того, как они все были добавлены.
     */
    public void disablePolling() {
        canPoll.set(true);
    }

    /**
     * Метод, включающий взятие задач из очереди.
     */
    public void enablePolling() {
        canPoll.set(false);
    }

    /**
     * Метод, ожидающий завершения выполнения всех задач в очереди.
     */
    public void waitForFinish() {
        while (!isFinished()) {
            Thread.yield();
        }
    }

    /**
     * @return закончено ли выполнение всех задач в очереди.
     */
    private boolean isFinished() {
        return inProgress.get() == 0 && tasksQueue.isEmpty();
    }

    /**
     * Метод подключения к коллаборативной очереди.
     *
     * Может быть вызван как потоком, инициировавшим блокирующую (коллаборативную) операцию,
     * так и другими потоками, которые не могут взять блокировку.
     *
     * В случае отсутствия задач для выполнения, поток сразу выйдет из метода,
     * в противном случае начнёт выполнять задачи, пока они есть.
     */
    public void helpIfNeeded() {
        if (tasksQueue.isEmpty() || !canPoll.get()) {
            return;
        }
        inProgress.incrementAndGet();
        while (true) {
            if (!canPoll.get()) {
                inProgress.incrementAndGet();
                return;
            }
            var task = tasksQueue.pollFirst();
            if (task == null) {
                inProgress.decrementAndGet();
                return;
            }
            if (!canPoll.get()) {
                tasksQueue.addFirst(task);
                inProgress.incrementAndGet();
                return;
            }
            task.start();
        }
    }

    /**
     * Метод, пытающийся взять блокировку, пока взятие не будет успешным.
     *
     * В случае успеха выполняет переданную функцию и возвращает её результат, отпуская блокироваку.
     *
     * В случае неуспеха пытается присоединиться к коллаборативной помощи.
     *
     * @param lock блокировка, которую необходимо получить, чтобы выполнить функцию
     * @param function функция, которую нужно выполнить после взятия блокировки
     * @return рассчитанное значение переданной функции
     * @param <T> тип возвращаемого функцией значения
     */
    public <T> T lockWithHelpIfNeeded(
            Lock lock,
            Supplier<T> function
    ) {
        while (true) {
            if (lock.tryLock()) {
                try {
                    return function.get();
                } finally {
                    lock.unlock();
                }
            } else {
                this.helpIfNeeded();
                Thread.yield();
            }
        }
    }

    public void lockWithHelpIfNeeded(
            Lock lock,
            Runnable function
    ) {
        while (true) {
            if (lock.tryLock()) {
                try {
                    function.run();
                    return;
                } finally {
                    lock.unlock();
                }
            } else {
                this.helpIfNeeded();
                Thread.yield();
            }
        }
    }

    public <T> T lockWithHelpIfNeeded() {
        while (true) {

        }
    }
}
