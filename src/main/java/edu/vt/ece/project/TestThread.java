package edu.vt.ece.project;

import java.util.concurrent.ThreadLocalRandom;

public class TestThread extends Thread {

    private static int COUNTER = 0;

    private int id;
    private int iter;
    private OpType opType;
    private BaseSet<Integer> set;
    volatile int successAdd = 0;
    volatile int totalAdd = 0;
    volatile int successRemove = 0;
    volatile int totalRemove = 0;
    volatile int totalContains = 0;
    volatile int eliminated = 0;
    volatile int combined = 0;
    private long elapsed;

    public TestThread(int iter, BaseSet<Integer> set, OpType type) {
        this.id = COUNTER++;
        this.iter = iter;
        this.set = set;
        this.opType = type;
    }

    @Override
    public void run() {
            ThreadLocalRandom random = ThreadLocalRandom.current();
            long start = System.currentTimeMillis();
            for(int i = 0; i < iter; i++) {
                int rand = random.nextInt(10000);
                if (opType == OpType.CONTAINS)
                    set.contains(rand);
                else if (opType == OpType.ADD)
                    set.add(rand);
                else
                    set.remove(rand);
            }
            elapsed = System.currentTimeMillis() - start;
    }

    public int getThreadId() {
        return this.id;
    }

    public int getSuccessAdd() {
        return successAdd;
    }

    public int getTotalAdd() {
        return totalAdd;
    }

    public int getSuccessRemove() {
        return successRemove;
    }

    public int getTotalRemove() {
        return totalRemove;
    }

    public int getTotalContains() {
        return totalContains;
    }

    public int getEliminated() {
        return eliminated;
    }

    public int getCombined() {
        return combined;
    }

    public long getElapsed() {
        return elapsed;
    }
}
