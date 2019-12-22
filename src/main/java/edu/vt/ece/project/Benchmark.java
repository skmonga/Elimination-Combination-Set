package edu.vt.ece.project;

public class Benchmark {

    private static final int NUM_BUCKETS = 8;

    public static void main(String[] args) throws Exception {
        final int threadCount = Integer.parseInt(args[0]);
        final int iters = Integer.parseInt(args[1]);
        final int containsPerc = Integer.parseInt(args[2]);
        int addPerc = 0;
        if(args.length > 3) {
            addPerc = Integer.parseInt(args[3]);
        }

        double tput = 0;
        int avgTime = 0;
        for(int j = 0; j < 5; j++) {
            ECSet<Integer> set = new ECSet<>(NUM_BUCKETS, 40000);

            ThreadOpDist opDistribution = ThreadOpDist.getDistribution(containsPerc, threadCount);
            if(opDistribution.containsExclIdx == 0) {
                //no contains used, only a write workload
                opDistribution.addExclIdx = (addPerc * threadCount)/100;
            }

            TestThread[] threads = new TestThread[threadCount];
            for (int i = 0; i < threadCount; i++) {
                if (i < opDistribution.containsExclIdx) {
                    threads[i] = new TestThread(iters, set, OpType.CONTAINS);
                } else if (i < opDistribution.addExclIdx) {
                    threads[i] = new TestThread(iters, set, OpType.ADD);
                } else {
                    threads[i] = new TestThread(iters, set, OpType.REMOVE);
                }
            }

            for (int i = 0; i < threadCount; i++) {
                threads[i].start();
            }

            long maxTime = 0;
            long totalTime = 0;
            for (int i = 0; i < threadCount; i++) {
                threads[i].join();
                maxTime = Math.max(maxTime, threads[i].getElapsed());
                totalTime += threads[i].getElapsed();
            }

            tput += (iters*threadCount) / (maxTime*0.001);
            avgTime += totalTime / threadCount;

//            set.printCounters();
            /*int successAdd = 0, totalAdd = 0, successRemove = 0, totalRemove = 0;
            int totalContains = 0;
            int eliminated = 0, combined = 0;
            for(int i = 0; i < threadCount; i++) {
                successAdd += threads[i].getSuccessAdd();
                totalAdd += threads[i].getTotalAdd();
                successRemove += threads[i].getSuccessRemove();
                totalRemove += threads[i].getTotalRemove();
                totalContains += threads[i].getTotalContains();
                eliminated += threads[i].getEliminated();
                combined += threads[i].getCombined();
            }

            System.out.println("============== STATS =============");
            System.out.println("Total Add Operations : " + totalAdd);
            System.out.println("Successful Add Operations : " + successAdd);
            System.out.println("Total Remove Operations : " + totalRemove);
            System.out.println("Successful Remove Operations : " + successRemove);
            System.out.println("Total Contains Operations : " + totalContains);
            System.out.println("Eliminated Operations : " + eliminated);
            System.out.println("Combined Operations : " + combined);*/
        }

        System.out.println("ECSet : Throughput is " + (tput/5) + "ops/s");
        System.out.println("ECSet : Average time per thread is " + (avgTime / 5*1.0) + "ms");


    }

    private static class ThreadOpDist {
        int containsExclIdx;
        int addExclIdx;

        public ThreadOpDist(int cIdx, int aIdx) {
            this.containsExclIdx = cIdx;
            this.addExclIdx = aIdx;
        }

        static ThreadOpDist getDistribution (int containsPerc, int threadCount) {
            int containsExclIdx = (containsPerc * threadCount)/100;
            int addExclIdx = containsExclIdx + (int) Math.ceil(((100-containsPerc) * threadCount * 1.0)/(2 * 100));
            return new ThreadOpDist(containsExclIdx, addExclIdx);
        }
    }

}
