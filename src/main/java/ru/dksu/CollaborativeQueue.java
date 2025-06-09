package ru.dksu;

import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.function.Consumer;
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
    private ConcurrentLinkedDeque<CollaborativeTask> tasksQueue = new ConcurrentLinkedDeque<>();
    private ConcurrentLinkedDeque<CollaborativeTask> nextTasksQueue = new ConcurrentLinkedDeque<>();
    private final AtomicInteger inProgress = new AtomicInteger(0);
    private final AtomicBoolean canPoll = new AtomicBoolean(true);
    private final boolean canEnablePollingWhenNotFinishPrevious;

    public CollaborativeQueue() {
        this(false);
    }

    public CollaborativeQueue(boolean canEnablePollingWhenNotFinishPrevious) {
        this.canEnablePollingWhenNotFinishPrevious = canEnablePollingWhenNotFinishPrevious;
    }

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
        if (canPoll.get()) {
            tasksQueue.addLast(task);
        } else {
            nextTasksQueue.addLast(task);
        }
    }

    /**
     * Метод, отключающий взятия задач из очереди.
     * <p>
     * Должен быть вызван до добавления первой задачи в случае, если
     * нельзя начать выполнение задач до того, как они все были добавлены.
     */
    public void disablePolling() {
        canPoll.set(false);
    }

    /**
     * Метод, включающий взятие задач из очереди.
     */
    public void enablePolling() {
        if (!canEnablePollingWhenNotFinishPrevious && !tasksQueue.isEmpty()) {
            throw new IllegalStateException(
                    "Queue of previous step wasn't clear when polling was enabled."
            );
        }
        tasksQueue.addAll(nextTasksQueue);
        nextTasksQueue.clear();
        canPoll.set(true);
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
     * @return закончено ли выполнение всех задач в текущей стадии очереди.
     */
    private boolean isFinished() {
        return inProgress.get() == 0 && tasksQueue.isEmpty();
    }

    /**
     * Метод подключения к коллаборативной очереди.
     * <p>
     * Может быть вызван как потоком, инициировавшим блокирующую (коллаборативную) операцию,
     * так и другими потоками, которые не могут взять блокировку.
     * <p>
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
     * <p>
     * В случае успеха выполняет переданную функцию и возвращает её результат, отпуская блокироваку.
     * <p>
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

    /**
     * Метод, пытающийся взять блокировку, пока взятие не будет успешным.
     * <p>
     * В случае успеха выполняет переданную функцию и возвращает её результат, отпуская блокироваку.
     * <p>
     * В случае неуспеха пытается присоединиться к коллаборативной помощи.
     *
     * @param lock блокировка, которую необходимо получить, чтобы выполнить функцию
     * @param function функция, которую нужно выполнить после взятия блокировки
     */
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

    /**
     * Метод, помогающий превратить блокирующую операцию в коллаборативную.
     *
     * @param lock блокировка, отвечающая за отсутствие параллельных модифицирующих операций
     * @param tasks функция, создающая и добавляющая коллаборативные задачи
     * @param reduce функция, объединяющая частичные результаты в итоговый результат
     * @return итоговое значение
     * @param <T> тип возвращаемого значения
     * @param <TempV> тип промежуточных результатов
     */
    public <T, TempV> T collaborativeOperation(
            Lock lock,
            Function<CollaborativeQueue<TaskT>, TempV> tasks,
            Function<TempV, T> reduce
    ) {
        while (true) {
            if (lock.tryLock()) {
                TempV tempResults;
                try {
                    tempResults = tasks.apply(this);
                    this.helpIfNeeded();
                    this.waitForFinish();
                } finally {
                    lock.unlock();
                }
                return reduce.apply(tempResults);
            } else {
                this.helpIfNeeded();
                Thread.yield();
            }
        }
    }

    /**
     * Метод, помогающий создать многоэтапную коллаборативную операцию.
     * <p>
     * Необходимо передать в функцию список блоков генерирующих подзадачи функций,
     * после завершения работы функции весь критический с точки зрения блокировки блок кода будет выполнен,
     * и необходимо будет собрать результат в итоговый.
     *
     * @param lock блокировка, отвечающая за отсутствие параллельных модифицирующих операций
     * @param tasksBlocks список функций, которые последовательно будут вызваны для создания задач
     */
    public void multiStageCollaborativeOperation(
            Lock lock,
            List<Consumer<CollaborativeQueue<TaskT>>> tasksBlocks
    ) {
        while (true) {
            if (lock.tryLock()) {
                try {
                    for (Consumer<CollaborativeQueue<TaskT>> tasks : tasksBlocks) {
                        tasks.accept(this);
                        this.helpIfNeeded();
                        this.waitForFinish();
                    }
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
}
