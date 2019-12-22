package edu.vt.ece.project;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicMarkableReference;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ECSet<T> extends BaseSet<T> {

    private static final int AVG_BUCKET_SIZE = 8;

    /* looks like tryLock can be useful in case the
       collision array is equal to the number of
       buckets in the table as a failure to acquire
       the lock can lead to better aggregation of
       requests
     */
    private ReentrantReadWriteLock[] locks;

    /* the number of elements here is equal to the
       number of threads or it can be initialized
       to a large value enough to hold values for
       all threads
     */
    private List<AtomicReference<OpChain>> contents;

    /* the size of collision array can have huge effect
       on the performance. If size is too big, then collision
       are rare which makes combining useless and if too small
       then elements from different buckets may come to the same
       place. For now, it is kept same as the number of buckets
     */
    /* optimization 2 : instead of a single collision place for
       every bucket, we have multiple places for each bucket and
       a thread randomly chooses an entry among the available places
       as a single entry for every bucket causes the CAS on collision
       to be serialized and may become a bottleneck
     */
    private List<AtomicReference<Integer>> collision;

    private static final int COL_ENTRY_PER_BUCKET = 10;

    /* this is resizing thread */
    private AtomicMarkableReference<Thread> resizing;

    /* the count of active chains per bucket */
    private List<AtomicInteger> chainCounts;

    private AtomicInteger setSize;

    public ECSet(int capacity, int n) {
        super(capacity);
        locks = new ReentrantReadWriteLock[capacity];
        contents = new ArrayList<>(n);
        int colSize = capacity * COL_ENTRY_PER_BUCKET;
        collision = new ArrayList<>(colSize);
        chainCounts = new ArrayList<>(capacity);
        for(int i = 0; i < capacity; i++) {
            locks[i] = new ReentrantReadWriteLock();
        }
        for(int i = 0; i < n; i++) {
            contents.add(new AtomicReference<>());
        }
        for(int i = 0; i < colSize; i++) {
            collision.add(new AtomicReference<>());
        }
        for(int i = 0; i < capacity; i++) {
            chainCounts.add(new AtomicInteger());
        }
        setSize = new AtomicInteger();
        resizing = new AtomicMarkableReference<Thread>(null, false);
    }

    private boolean timeForResize() {
        if(setSize.get()/table.length > AVG_BUCKET_SIZE)
            return true;
        return false;
    }

    public void resize() {
        int oldCapacity = table.length;
        int newCapacity = 2 * oldCapacity;
        Thread me = Thread.currentThread();
        if (resizing.compareAndSet(null, me, false, true)) {
            try {
                if (table.length != oldCapacity) {
                    return;
                }
                quiesce();

                List<T>[] oldTable = table;
                table = (List<T>[]) new List[newCapacity];
                for (int i = 0; i < newCapacity; i++)
                    table[i] = new ArrayList<T>();

                locks = new ReentrantReadWriteLock[newCapacity];
                for (int i = 0; i < locks.length; i++) {
                    locks[i] = new ReentrantReadWriteLock();
                }

                int colSize = newCapacity * COL_ENTRY_PER_BUCKET;
                collision = new ArrayList<>(colSize);
                for(int i = 0; i < colSize; i++) {
                    collision.add(new AtomicReference<>());
                }

                chainCounts = new ArrayList<>(newCapacity);
                for(int i = 0; i < newCapacity; i++) {
                    chainCounts.add(new AtomicInteger());
                }

                moveEntries(oldTable);
            } finally {
                resizing.set(null, false);
            }
//            System.out.println("=========== RESIZING DONE ===========");
        }
    }

    private void quiesce() {
        for (ReentrantReadWriteLock lock : locks) {
            while (lock.isWriteLocked() || lock.getReadLockCount() != 0) {
            }
        }

        /* locks are released which can acquired later by
           the running chains. We will allow chains of length
           > 1 to complete first before resizing
         */
        for(AtomicInteger value : chainCounts) {
            while(value.get() != 0) {}
        }
    }

    private void moveEntries(List<T>[] oldTable) {
        for(List<T> bucket : oldTable) {
            for(T entry : bucket) {
                table[Math.abs(entry.hashCode()) % table.length].add(entry);
            }
        }
    }

    private boolean tryWriteAcquire(OpChain chain) {
        boolean[] mark = {true};
        Thread me = Thread.currentThread();
        Thread resize = resizing.get(mark);
        if(mark[0] && resize != me && chain.length == 1)
            return false;
        ReentrantReadWriteLock[] oldLocks = locks;
        ReentrantReadWriteLock oldLock = oldLocks[Math.abs(chain.data.hashCode()) % oldLocks.length];
        /* it is possible that a resize may complete from the
           last time we check (if condition above) and we read
           the locks array above. However a new instance of lock
           may not be created and check for null lock in the new array
         */
        if(oldLock == null || !oldLock.writeLock().tryLock())
            return false;
        resize = resizing.get(mark);
        if ((!mark[0] || resize == me || chain.length > 1) && locks == oldLocks) {
            return true;
        } else {
            oldLock.writeLock().unlock();
            return false;
        }
    }

    private void writeAcquire(OpChain chain) {
        while(true) {
            boolean[] mark = {true};
            Thread me = Thread.currentThread();
            Thread resize;
            do {
                resize = resizing.get(mark);
            } while (mark[0] && resize != me && chain.length == 1);
            ReentrantReadWriteLock[] oldLocks = locks;
            ReentrantReadWriteLock oldLock = oldLocks[Math.abs(chain.data.hashCode()) % oldLocks.length];
            if (oldLock != null)
                oldLock.writeLock().lock();
            resize = resizing.get(mark);
            if ((!mark[0] || resize == me || chain.length > 1) && locks == oldLocks) {
               return;
            } else {
                if(oldLock != null) {
                    oldLock.writeLock().unlock();
                }
            }
        }
    }

    public boolean add(T element) {
        int threadId = ((TestThread) Thread.currentThread()).getThreadId();
        OpChain myChain = OpChain.init(threadId, OpType.ADD, element);
        boolean response = false;
        while(true) {
            if (myChain.ecAttempts >= OpChain.MAX_EC_ATTEMPTS) {
                writeAcquire(myChain);
                performOperation(myChain);
                response = parseAddResponse(myChain);
                break;
            } else {
                if (tryWriteAcquire(myChain)) {
                    performOperation(myChain);
                    response = parseAddResponse(myChain);
                    break;
                } else {
                    if (tryCollision(myChain)) {
                        response = parseAddResponse(myChain);
                        break;
                    }
                }
            }
        }
        if(timeForResize()) {
            resize();
        }
        return response;
    }

    @Override
    public boolean remove(T element) {
        int threadId = ((TestThread) Thread.currentThread()).getThreadId();
        OpChain myChain = OpChain.init(threadId, OpType.REMOVE, element);

        while (true) {
            if (myChain.ecAttempts >= OpChain.MAX_EC_ATTEMPTS) {
                writeAcquire(myChain);
                performOperation(myChain);
                return parseAddResponse(myChain);
            } else {
                if (tryWriteAcquire(myChain)) {
                    performOperation(myChain);
                    return parseRemoveResponse(myChain);
                } else {
                    if (tryCollision(myChain))
                        return parseRemoveResponse(myChain);
                }
            }
        }
    }

    private void readAcquire(T element) {
        while(true) {
            boolean[] mark = {true};
            Thread me = Thread.currentThread();
            Thread resize;
            do {
                resize = resizing.get(mark);
            } while (mark[0] && resize != me);
            ReentrantReadWriteLock[] oldLocks = locks;
            ReentrantReadWriteLock oldLock = oldLocks[Math.abs(element.hashCode()) % oldLocks.length];
            if (oldLock != null)
                oldLock.readLock().lock();
            resize = resizing.get(mark);
            if ((!mark[0] || resize == me) && locks == oldLocks) {
                return;
            } else {
                if(oldLock != null) {
                    oldLock.readLock().unlock();
                }
            }
        }
    }

    @Override
    public boolean contains(T element) {
        boolean result = false;
        TestThread me = (TestThread) Thread.currentThread();
        readAcquire(element);
        int bucket = Math.abs(element.hashCode()) % table.length;
        result = table[bucket].contains(element);
        locks[bucket].readLock().unlock();
//        me.totalContains += 1;
        return result;

    }

    /* writeLock is held when calling this method
       and released in this method*/
    /* TODO: comment the volatile counter increments */
    private void performOperation(OpChain chain) {
        /* keeping count to update the size of the set */
        int successCount = 0;
        T self = (T) chain.data;
        int bucket = Math.abs(self.hashCode()) % table.length;
        while(chain.next != null) {
            T element = (T) chain.next.data;
            OpType operationType = chain.next.oType;
            if(operationType == OpType.ADD) {
                if(table[bucket].contains(element)) {
                    chain.next.respType = ResponseType.ADD_ALREADY_EXISTS;
                } else {
                    table[bucket].add(element);
                    successCount += 1;
                    chain.next.respType = ResponseType.ADD_SUCCESS;
//                    chain.next.thread.successAdd += 1;
                }
//                chain.next.thread.totalAdd += 1;
            } else if (operationType == OpType.REMOVE) {
                if(!table[bucket].contains(element)) {
                    chain.next.respType = ResponseType.REMOVE_NOT_EXISTS;
                } else {
                    table[bucket].remove(element);
                    successCount -= 1;
                    chain.next.respType = ResponseType.REMOVE_SUCCESS;
//                    chain.next.thread.successRemove += 1;
                }
//                chain.next.thread.totalRemove += 1;
            }
            chain.next.status = OpStatus.DONE;
            chain.next = chain.next.next;
        }

        OpType operationType = chain.oType;
        if(operationType == OpType.ADD) {
            if(table[bucket].contains(self)) {
                chain.respType = ResponseType.ADD_ALREADY_EXISTS;
            } else {
                table[bucket].add(self);
                successCount += 1;
                chain.respType = ResponseType.ADD_SUCCESS;
//                chain.thread.successAdd += 1;
            }
//            chain.thread.totalAdd += 1;
        } else if (operationType == OpType.REMOVE) {
            if(!table[bucket].contains(self)) {
                chain.respType = ResponseType.REMOVE_NOT_EXISTS;
            } else {
                table[bucket].remove(self);
                successCount -= 1;
                chain.respType = ResponseType.REMOVE_SUCCESS;
//                chain.thread.successRemove += 1;
            }
//            chain.thread.totalRemove += 1;
        }
        setSize.getAndAdd(successCount);
//        chain.thread.combined += (chain.length > 1)?chain.length:0;
        chain.status = OpStatus.DONE;
        /* release the lock first and then decrement the chain count
           as a resizing thread may now be waiting on quiesce() on
           the chain count condition
         */
        locks[bucket].writeLock().unlock();

        /* once all requests applied, remove from the running
           chains as resize thread waits on it
         */
        if(chain.length > 1)
            chainCounts.get(chain.chainBucket).getAndDecrement();
    }

    private boolean matchingBuckets(OpChain chain1, OpChain chain2) {
        int bucket1 = Math.abs(chain1.data.hashCode()) % table.length;
        int bucket2 = Math.abs(chain2.data.hashCode()) % table.length;
        return bucket1 == bucket2;
    }

    private int getCollisionIdx(OpChain chain) {
        int baseIdx = (Math.abs(chain.data.hashCode()) % table.length)
                            * COL_ENTRY_PER_BUCKET;
        ThreadLocalRandom random = ThreadLocalRandom.current();
        int offsetIdx = random.nextInt(COL_ENTRY_PER_BUCKET);
        return baseIdx + offsetIdx;
    }

    private boolean tryCollision(OpChain chain) {
        chain.ecAttempts += 1;
        boolean[] mark = {true};
        Thread me = Thread.currentThread();
        Thread resize = resizing.get(mark);
        if(mark[0] && resize != me)
            return false;
        int threadId = ((TestThread) me).getThreadId();
        contents.get(threadId).set(chain);
//        int colIdx = Math.abs(chain.data.hashCode()) % table.length;
        int colIdx = getCollisionIdx(chain);
        /* check if in progress resize has not applied
           all changes to the collision array
         */
//        AtomicReference<Integer>[] oldCollision = collision;
        List<AtomicReference<Integer>> oldCollision = collision;
        /* either the collision array not resized when we read it OR
           it was resized but elements were not initialized
         */
        if(colIdx >= oldCollision.size() || oldCollision.get(colIdx) == null)
            return false;
        Integer waiterId = oldCollision.get(colIdx).get();
        while(!oldCollision.get(colIdx).compareAndSet(waiterId, threadId)) {
            waiterId = oldCollision.get(colIdx).get();
        }
        if(waiterId != null) {
            OpChain passive = contents.get(waiterId).get();
            if(passive != null && passive.id != threadId &&
                    passive.id == waiterId && matchingBuckets(chain, passive)) {
                if(contents.get(threadId).compareAndSet(chain, null)) {
                    return startCollision(chain, passive);
                } else {
                    return waitForResponse(chain);
                }
            }
        }
        waitForCollider();
        if(!contents.get(threadId).compareAndSet(chain, null)) {
            return waitForResponse(chain);
        }
        return false;
    }

    /* the strategy of AtomicMarkableReference works because every
       thread first does a write to a field of its interest and reads
       the other to check for some condition. Here during combining,
       a similar strategy is used where we first write to the running
       chains for our bucket and then do a read to check for resizing.
       If resizing is in progress, then we will remove (if needed i.e.
       chain length == 1)
     */
    private void combineChains(OpChain active, OpChain passive) {

        if(active.length == 1) {
            /* if length is > 1, then I am already in the running
               chains. However it may happen that active may become
               passive later which is done by removing passive from
               the running chains
             */
            int activeBucket = Math.abs(active.data.hashCode()) % table.length;
            if(activeBucket >= chainCounts.size() || chainCounts.get(activeBucket) == null)
                return;
            chainCounts.get(activeBucket).getAndIncrement();
            active.chainBucket = activeBucket;
        }
        if(passive.length > 1) {
            chainCounts.get(passive.chainBucket).getAndDecrement();
        }

        /* we want to extend the chain only if there is no resizing
           thread present at the moment
         */
        boolean[] mark = {true};
        Thread me = Thread.currentThread();
        Thread resize = resizing.get(mark);
        if(mark[0] && resize != me) {
            /* a thread is interested in resizing so don't extend
               the chain. All threads in active chain will be served
               by active before resizing assuming length of active
               chain is > 1. However passive is gone to waiting now
               and will be woken up by active which won't happen as
               active is not going to extend the chain any further. So
               to handle that case, signal passive to stop its waiting
             */
            if(passive.length > 1) {
                /* chains are not extended but existing ones must
                   finish by completing their requests
                 */
                chainCounts.get(passive.chainBucket).getAndIncrement();
            }
            passive.status = OpStatus.RETRY;
            if(active.length == 1) {
                /* don't create single element chains during resizing */
                chainCounts.get(active.chainBucket).getAndDecrement();
                active.chainBucket = -1;
            }
            return;
        } else {
            /* it is possible that a resize may finish between the time
               we collide and do the combining part, so make sure that's
               not the case as the buckets for collision might have changed.
               This case may occur only when both active and passive have
               only one element since in other cases, there will be one or
               both of them in the running chains.
             */
            int activeBucket = Math.abs(active.data.hashCode()) % table.length;
            int passiveBucket = Math.abs(passive.data.hashCode()) % table.length;
            if(activeBucket != passiveBucket) {
                /* this can only happen if both active and passive
                   chains are of length 1
                 */
                System.out.println(activeBucket + " , " + passiveBucket);
                /* buckets have changed so no need to combine */
                chainCounts.get(active.chainBucket).getAndDecrement();
                active.chainBucket = -1;
                passive.status = OpStatus.RETRY;
                return;
            } else {
                /* this code block can be reached via the regular path
                   when one or both of the chains contain more than one
                   element. That is a common case and is of no specific
                   interest. The other case of both chains containing
                   one element and now combining has a corner case as
                   we initially incremented the count for the active chain
                   However a resize might be in progress during the time
                   which may lead to a bucket change for the element. So
                   to handle that case, we will increment counter specific
                   to the new bucket for active and remove from the old one
                 */
                if(active.length == 1 && passive.length == 1) {
                    if(activeBucket != active.chainBucket) {
                        /* change in the bucket for active (and passive) */
                        System.out.println("========== BUCKET CHANGE ==========");
                        chainCounts.get(activeBucket).getAndIncrement();
                        chainCounts.get(active.chainBucket).getAndDecrement();
                        active.chainBucket = activeBucket;
                    }
                }
            }
        }

        if(active.next == null) {
            active.next = passive;
        } else {
            active.last.next = passive;
        }
        if(passive.next == null) {
            active.last = passive;
        } else {
            active.last = passive.last;
        }
        active.length += passive.length;
        passive.chainBucket = -1;
    }

    /* TODO: comment the volatile counter increments */
    private void tryEliminate(OpChain active, OpChain passive) {
        /* these init references are kept as these may be present
           in the running chains but might get eliminated during
           this execution, so their chainBuckets will be passed on
           to the elements subsequently leading the chain
         */
        OpChain initActive = active, initPassive = passive;
        int initActiveLength = active.length, initPassiveLength = passive.length;

        OpChain aCurr = active, aPrev = null;
        OpChain pCurr = passive, pPrev = null;
        /* for every element in aList, try to eliminate using some
           element from pList
         */
        while(aCurr != null && pCurr != null) {
            boolean isEliminated = false;
            T aElement = (T) aCurr.data;
            while(pCurr != null) {
                T pElement = (T) pCurr.data;
                if(aElement.equals(pElement)) {
                    /* eliminate both of them */
                    isEliminated = true;
//                    pCurr.status = OpStatus.DONE;
                    passive.length -= 1;
//                    aCurr.status = OpStatus.DONE;
                    active.length -= 1;
                    if(pCurr.oType == OpType.ADD) {
                        pCurr.respType = ResponseType.ADD_SUCCESS;
//                        pCurr.thread.successAdd += 1;
//                        pCurr.thread.totalAdd += 1;
                        aCurr.respType = ResponseType.REMOVE_SUCCESS;
//                        aCurr.thread.successRemove += 1;
//                        aCurr.thread.totalRemove += 1;
                    } else {
                        pCurr.respType = ResponseType.REMOVE_SUCCESS;
//                        pCurr.thread.successRemove += 1;
//                        pCurr.thread.totalRemove += 1;
                        aCurr.respType = ResponseType.ADD_SUCCESS;
//                        aCurr.thread.successAdd += 1;
//                        aCurr.thread.totalAdd += 1;
                    }
//                    pCurr.thread.eliminated += 1;
//                    aCurr.thread.eliminated += 1;

                    /* if this is the last element, update last in chain */
                    /* for 2 element chain with last element eliminated,
                       update last to null
                     */
                    if(pCurr.next == null) {
                        passive.last = (pPrev != passive)?pPrev:null;
                    }
                    if(pPrev != null) {
                        pPrev.next = pCurr.next;
                    } else {
                        if(passive.next != null) {
                            passive.next.length = passive.length;
                            passive.next.last = (passive.last != passive.next)?passive.last:null;
                        }
                        passive = passive.next;
                    }

                    /* if this is the last element, update last in chain */
                    if(aCurr.next == null) {
                        active.last = (aPrev != active)?aPrev:null;
                    }
                    if(aPrev != null) {
                        aPrev.next = aCurr.next;
                    } else {
                        if(active.next != null) {
                            active.next.length = active.length;
                            active.next.last = (active.last != active.next)?active.last:null;
                        }
                        active = active.next;
                    }
                    pCurr.status = OpStatus.DONE;
                    aCurr.status = OpStatus.DONE;
                    break;
                }
                pPrev = pCurr;
                pCurr = pCurr.next;
            }

            /* change aPrev only if there is no elimination */
            if(!isEliminated) {
                aPrev = aCurr;
            }
            aCurr = aCurr.next;
            pPrev = null;
            pCurr = passive;
        }

        if(active != null) {
            if(active == initActive) {
                if(active.length == 1 && active.chainBucket != -1) {
                    /* removing only to make a resizing thread progress */
                    chainCounts.get(active.chainBucket).getAndDecrement();
                    active.chainBucket = -1;
                }
            } else {
                if(active.length > 1) {
                    /* residual chain has more than one element so a resizing
                       thread needs to wait
                     */
                    active.chainBucket = initActive.chainBucket;
                } else {
                    chainCounts.get(initActive.chainBucket).getAndDecrement();
                }
            }
            active.status = OpStatus.RETRY;
        } else {
            if(initActiveLength > 1) {
                chainCounts.get(initActive.chainBucket).getAndDecrement();
            }
        }

        if(passive != null) {
            if(passive == initPassive) {
                if(passive.length == 1 && passive.chainBucket != -1) {
                    chainCounts.get(passive.chainBucket).getAndDecrement();
                    passive.chainBucket = -1;
                }
            } else {
                if(passive.length > 1) {
                    passive.chainBucket = initPassive.chainBucket;
                } else {
                    chainCounts.get(initPassive.chainBucket).getAndDecrement();
                }
            }
            passive.status = OpStatus.RETRY;
        } else {
            if(initPassiveLength > 1) {
                chainCounts.get(initPassive.chainBucket).getAndDecrement();
            }
        }
    }

    /* this is the combiner thread */
    private boolean startCollision(OpChain active, OpChain passive) {
        if(contents.get(passive.id).compareAndSet(passive, active)) {
            if(active.oType == passive.oType) {
                combineChains(active, passive);
                return false;
            } else {
                tryEliminate(active, passive);
                if(active.status == OpStatus.DONE)
                    return true;
//                passive.status = OpStatus.RETRY;
            }
        }
        active.status = OpStatus.INIT;
        return false;
    }

    /* this thread's request will be tried by the combiner thread */
    private boolean waitForResponse(OpChain chain) {
        OpChain active = contents.get(chain.id).get();
        contents.get(chain.id).set(null);

        while(chain.status == OpStatus.INIT) {
//            System.out.println("I am passive");
        }
        if(chain.status == OpStatus.DONE) {
            return true;
        } else {
            /* need to retry */
            chain.status = OpStatus.INIT;
            return false;
        }
    }

    private void waitForCollider() {
       /*try {
            Thread.sleep(0,100);
        } catch (InterruptedException ex) {

        }*/
        for(int i = 200; i > 0; i--);
    }

    private boolean parseAddResponse(OpChain chain) {
        if(chain.respType == ResponseType.ADD_ALREADY_EXISTS)
            return false;
        else
            return true;
    }

    private boolean parseRemoveResponse(OpChain chain) {
        if(chain.respType == ResponseType.REMOVE_NOT_EXISTS)
            return false;
        else
            return true;
    }

    public void printCounters() {
        int i = 0;
        int count = 0;
        for(List<T> bucket : table) {
            int currCount = bucket.size();
            System.out.println("Size of Bucket " + i++ + " : " + currCount);
            count += currCount;
        }
        System.out.println("Total elements in the set is : " + count);
    }

}

