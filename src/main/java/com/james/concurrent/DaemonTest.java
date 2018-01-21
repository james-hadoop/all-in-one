package com.james.concurrent;

import java.util.ArrayDeque;
import java.util.Date;
import java.util.Deque;
import java.util.concurrent.TimeUnit;

public class DaemonTest {
    public static void main(String[] args) {
        // Deque
        Deque<Event> queue = new ArrayDeque<Event>();

        // WriterTask
        WriterTask writer = new WriterTask(queue);
        for (int i = 0; i < 3; i++) {
            Thread thread = new Thread(writer);
            thread.start();
        }

        // CleanerTask
        CleanerTask cleaner = new CleanerTask(queue);
        cleaner.start();
    }
}

class Event {
    Date date = null;
    String event = null;

    public Date getDate() {
        return date;
    }

    public void setDate(Date data) {
        this.date = data;
    }

    public String getEvent() {
        return event;
    }

    public void setEvent(String event) {
        this.event = event;
    }
}

class WriterTask implements Runnable {
    private Deque<Event> deque;

    public WriterTask(Deque<Event> deque) {
        this.deque = deque;
    }

    public void run() {
        for (int i = 0; i < 10; i++) {
            Event event = new Event();
            event.setDate(new Date());
            event.setEvent(String.format("The thread %s has generated an event", Thread.currentThread().getId()));

            deque.addFirst(event);

            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}

class CleanerTask extends Thread {
    private Deque<Event> deque;

    public CleanerTask(Deque<Event> deque) {
        this.deque = deque;
        
        /*
         * JVM will exits if all running threads are all daemon threads
         */
        setDaemon(true);
    }

    @Override
    public void run() {
        while (true) {
            System.out.println("run() in daemon thread.");
            
            Date date = new Date();
            clean(date);
        }
    }

    private void clean(Date date) {
        long difference;
        boolean delete;

        if (deque.size() == 0) {
            return;
        }

        delete = false;

        do {
            Event event = deque.getLast();
            difference = date.getTime() - event.getDate().getTime();

            if (difference > 10000) {
                System.out.printf("Cleaner: %s\n", event.getEvent());
                deque.removeLast();
                delete = true;
            }
        } while (difference > 10000);

        if (delete) {
            System.out.printf("Cleaner: Size of the queue: %d\n", deque.size());
        }
    }
}
