package edu.vt.ece.project;

public class OpChain<T> {

    int id;
    /* Adding the TestThread for debugging purpose */
    TestThread thread;
    OpType oType;
    volatile OpStatus status = OpStatus.INIT;
    //TODO:: need to have an upper bound on chain length
    volatile int length;
    T data;
    volatile OpChain<T> next, last;
    ResponseType respType;
    /* this is the bucket number for this chain when
       the executing thread becomes the leader for a
       chain of length > 1
     */
    volatile int chainBucket = -1;
    int ecAttempts = 0;
    static final int MAX_EC_ATTEMPTS = 5;

    public OpChain(int _id, OpType _type, T _data) {
        this.id = _id;
        this.oType = _type;
        this.status = OpStatus.INIT;
        this.length = 1;
        this.data = _data;
        this.next = null;
        this.last = null;
        this.thread = (TestThread)Thread.currentThread();
    }

    public static <T> OpChain init(int _id, OpType _type, T _data) {
        return new OpChain(_id, _type, _data);
    }

    public void setThread(TestThread thread) {
        this.thread = thread;
    }


    @Override
    public int hashCode() {
        return super.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if(obj == null)
            return false;
        OpChain other = (OpChain) obj;
        return this.id == other.id && this.data.equals(other.data);
    }

}
