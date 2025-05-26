package ru.dksu;

/**
 * Интерфейс, предназначенный для создания коллаборативных задач.
 *
 * @author DenisKorotchenko
 */
public interface CollaborativeTask {
    /**
     * Основной метод, который будет выполнен при получении потоком задачи на исполнение.
     */
    public void start();
}
