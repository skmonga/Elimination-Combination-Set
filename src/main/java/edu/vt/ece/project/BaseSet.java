package edu.vt.ece.project;

import java.util.ArrayList;
import java.util.List;

public abstract class BaseSet<T> {

    protected List<T>[] table;

    public BaseSet(int capacity) {
        table = (List<T>[]) new List[capacity];
        for (int i = 0; i < capacity; i++) {
            table[i] = new ArrayList<T>();
        }
    }

    /* return true if this element was not already present
       else return false
     */
    public abstract boolean add(T element);

    /* return true if this element was present in the set
       else return false
     */
    public abstract boolean remove(T element);

    /* return true if the set contains this element */
    public abstract boolean contains(T element);

}
