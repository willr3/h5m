package io.hyperfoil.tools.h5m.queue;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

public class Counters<T> {


    private final ConcurrentHashMap<T, AtomicInteger> counters;
    private Map<String, Consumer<T>> callbacks = new ConcurrentHashMap<>();

    public Counters(){
        this.counters = new ConcurrentHashMap<>();
    }

    public boolean hasCallback(String name){
        return callbacks.containsKey(name);
    }
    public void setCallback(String name, Consumer<T> callback){
        callbacks.put(name, callback);
    }
    public void removeCallback(String name){
        callbacks.remove(name);
    }

    public boolean hasKey(T name){
        return counters.containsKey(name);
    }
    public int get(T name){
        return hasKey(name) ? counters.get(name).get() : 0;
    }
    public int increment(T item){
        return counters.computeIfAbsent(item, k -> new AtomicInteger(0)).incrementAndGet();
    }
    public int decrement(T item){
        int newvalue = counters.computeIfAbsent(item, k -> new AtomicInteger(0)).decrementAndGet();
        if(newvalue == 0){
            callbacks.forEach((k,v)->v.accept(item));
        }
        return newvalue;
    }
    public Set<T> getKeys(){
        return counters.keySet();

    }
    public int sum(){
        return counters.values().stream().mapToInt(AtomicInteger::get).sum();
    }



}
