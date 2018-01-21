package com.james.concurrent.assist;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Exchanger;

public class ExchangerTest {
    public static void main(String[] args) {
        List<String> buffer1 = new ArrayList<String>();
        List<String> buffer2 = new ArrayList<String>();

        Exchanger<List<String>> exchanger = new Exchanger<List<String>>();

        ExchangerProducer producer = new ExchangerProducer(buffer1, exchanger);
        ExchangerConsumer consumer = new ExchangerConsumer(buffer2, exchanger);

        new Thread(producer).start();
        new Thread(consumer).start();
    }
}

class ExchangerProducer implements Runnable {
    private List<String> buffer;
    private final Exchanger<List<String>> exchanger;

    public ExchangerProducer(List<String> buffer, Exchanger<List<String>> exchanger) {
        this.buffer = buffer;
        this.exchanger = exchanger;
    }

    public void run() {
        int cycle = 1;

        for (int i = 0; i < 10; i++) {
            System.out.printf("Producer: Cycle %d\n", cycle);

            for (int j = 0; j < 10; j++) {
                String message = "Event " + ((i * 10) + j);
                System.out.printf("Producer: %s\n", message);
                buffer.add(message);

                try {
                    /*
                     * Waits for another thread to arrive at this exchange point (unless the current
                     * thread is interrupted), and then transfers the given object to it, receiving
                     * its object in return.
                     */
                    buffer = exchanger.exchange(buffer);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            System.out.println("Producer: " + buffer.size());
            cycle++;
        }
    }
}

class ExchangerConsumer implements Runnable {
    private List<String> buffer;
    private final Exchanger<List<String>> exchanger;

    public ExchangerConsumer(List<String> buffer, Exchanger<List<String>> exchanger) {
        this.buffer = buffer;
        this.exchanger = exchanger;
    }

    public void run() {
        int cycle = 1;

        for (int i = 0; i < 10; i++) {
            System.out.printf("Consumer: Cycle %d\n", cycle);

            try {
                buffer = exchanger.exchange(buffer);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            System.out.println("Consumer: " + buffer.size());

            for (int j = 0; j < 10; j++) {
                String message = buffer.get(0);
                System.out.println("Consumer: " + message);
                buffer.remove(0);

                cycle++;
            }
        }
    }
}
