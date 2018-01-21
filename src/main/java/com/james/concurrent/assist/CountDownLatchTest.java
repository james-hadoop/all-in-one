package com.james.concurrent.assist;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class CountDownLatchTest {
    public static void main(String[] args) {
        VideoConference conference = new VideoConference(10);

        Thread threadConference = new Thread(conference);
        threadConference.start();

        for (int i = 0; i < 10; i++) {
            Participant p = new Participant(conference, "Participant " + i);
            new Thread(p).start();
        }
    }
}

class VideoConference implements Runnable {
    /*
     * A synchronization aid that allows one or more threads to wait until a set of
     * operations being performed in other threads completes.
     */
    private final CountDownLatch controller;

    public VideoConference(int number) {
        controller = new CountDownLatch(number);
    }

    public void arrive(String name) {
        System.out.printf("%s has arrived.", name);
        /*
         * Decrements the count of the latch, releasing all waiting threads if the count
         * reaches zero.
         */
        controller.countDown();

        System.out.printf("VideoConference: Waiting for %d participants.\n", controller.getCount());
    }

    public void run() {
        System.out.printf("VideoConference: Initialization: %d participants.\n", controller.getCount());

        try {
            /*
             * Causes the current thread to wait until the latch has counted down to zero,
             * unless the thread is interrupted.
             */
            controller.await();

            System.out.printf("VideoConference: All participants have come\n");
            System.out.printf("VideoConference: Let's start...\n");
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}

class Participant implements Runnable {
    private VideoConference conference;
    private String name;

    public Participant(VideoConference conference, String name) {
        this.conference = conference;
        this.name = name;
    }

    public void run() {
        long duration = (long) (Math.random() * 10);
        try {
            TimeUnit.SECONDS.sleep(duration);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        conference.arrive(name);
    }
}
