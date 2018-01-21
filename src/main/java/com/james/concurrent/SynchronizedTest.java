package com.james.concurrent;

public class SynchronizedTest {
    public static void main(String[] args) {
        Account account = new Account();
        account.setBalance(1000);

        Company company = new Company(account);
        Thread companyThread = new Thread(company);

        Bank bank = new Bank(account);
        Thread bankThread = new Thread(bank);

        System.out.printf("Account : Intial Balance: %f\n", account.getBalance());
        companyThread.start();
        bankThread.start();

        try {
            companyThread.join();
            bankThread.join();
            System.out.printf("Account : Final Balance: %f\n", account.getBalance());
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}

class Account {
    private double balance;

    public double getBalance() {
        return balance;
    }

    public void setBalance(double balance) {
        this.balance = balance;
    }

    /*
     * synchronized addAmount()
     */
    public synchronized void addAmount(double amount) {
        System.out.println("addAmount()");
        double tmp = balance;

        try {
            Thread.sleep(10);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        tmp += amount;
        balance = tmp;
    }

    /*
     * synchronized subtractAmount()
     */
    public synchronized void subtractAmount(double amount) {
        System.out.println("subtractAmount()");

        double tmp = balance;

        try {
            Thread.sleep(10);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        tmp -= amount;
        balance = tmp;
    }
}

class Bank implements Runnable {
    private Account account;

    public Bank(Account account) {
        this.account = account;
    }

    public void run() {
        for (int i = 0; i < 100; i++) {
            account.subtractAmount(1000);
        }
    }
}

class Company implements Runnable {
    private Account account;

    public Company(Account account) {
        this.account = account;
    }

    public void run() {
        for (int i = 0; i < 100; i++) {
            account.addAmount(1000);
        }
    }
}