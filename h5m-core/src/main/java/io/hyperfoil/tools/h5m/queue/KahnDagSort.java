package io.hyperfoil.tools.h5m.queue;

import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

//based on https://github.com/williamfiset/Algorithms/blob/master/src/main/java/com/williamfiset/algorithms/graphtheory/Kahns.java
public class KahnDagSort {
    /**
     * Performs a KahnDag sort of the items in T preserving current relative order for un-related entries.
     * T must have a unique hashCode and proper equals method for each entry as collisions will cause items to be dropped.
     * @param list
     * @param getDependencies function that calculates the dependencies of the input item
     * @return
     * @param <T>
     */
    public static <T> List<T> sort(List<T> list, Function<T,List<T>> getDependencies){
        Map<T, AtomicInteger> inDegrees = new HashMap<>();
        if(list == null || list.isEmpty()){
            return list;
        }
        list.forEach(t -> {
            inDegrees.put(t, new AtomicInteger(0));

        });
        list.forEach(t->{
            getDependencies.apply(t)
                    .forEach(s->{
                        if(inDegrees.containsKey(s)){
                            inDegrees.get(s).incrementAndGet();
                        }
                    });
        });
        Queue<T> q = new ArrayDeque<>();
        //using reveresed to preserve order
        list.reversed().forEach(t -> {
            if(inDegrees.get(t).intValue() == 0){
                q.offer(t);
            }
        });
        List<T> rtrn = new ArrayList<>();
        while(!q.isEmpty()){
            T t = q.poll();
            rtrn.add(t);
            getDependencies.apply(t)
                .forEach(s->{
                    if(inDegrees.containsKey(s)){
                        int newDegree = inDegrees.get(s).decrementAndGet();
                        if(newDegree == 0){
                            q.offer(s);
                        }
                    }
                });
        }
        int sum = inDegrees.values().stream().map(AtomicInteger::get).reduce(Integer::sum).orElse(0);
        if(sum > 0){
            //this means there are loops!!
            //using reversed to preserve order
            list.reversed().forEach(n->{
                if(inDegrees.get(n).get() > 0){
                    rtrn.add(0,n);//they will then go to the back
                }
            });
        }
        Collections.reverse(rtrn);
        return new CopyOnWriteArrayList<>(rtrn);
    }

    public static <T> boolean isCircular(T item,Function<T,List<T>> getDependencies){
        if(item == null){
            return false;
        }
        List<T> dependenices = getDependencies.apply(item);
        if(dependenices.isEmpty()){
            return false;
        }
        Queue<T> q = new PriorityQueue<>(dependenices);
        T target;
        boolean ok = true;
        while(ok && (target=q.poll())!=null){
            if(item.equals(target)){
                ok = false;
            }
            q.addAll(getDependencies.apply(target));
        }
        return !ok;
    }
}
