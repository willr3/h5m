package io.hyperfoil.tools.h5m.queue;

import java.util.*;
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
        if(list == null || list.isEmpty()){
            return list;
        }
        // Pre-compute adjacency map: call getDependencies once per item instead of twice
        Map<T, List<T>> adjacencyMap = new HashMap<>(list.size() * 2);
        for (int i = 0, size = list.size(); i < size; i++) {
            T item = list.get(i);
            adjacencyMap.put(item, getDependencies.apply(item));
        }
        Map<T, AtomicInteger> inDegrees = new HashMap<>(list.size() * 2);
        for (int i = 0, size = list.size(); i < size; i++) {
            inDegrees.put(list.get(i), new AtomicInteger(0));
        }
        for (int i = 0, size = list.size(); i < size; i++) {
            List<T> deps = adjacencyMap.get(list.get(i));
            for (int j = 0, dSize = deps.size(); j < dSize; j++) {
                AtomicInteger degree = inDegrees.get(deps.get(j));
                if (degree != null) {
                    degree.incrementAndGet();
                }
            }
        }
        Queue<T> q = new ArrayDeque<>();
        //using reversed to preserve order
        for (int i = list.size() - 1; i >= 0; i--) {
            if (inDegrees.get(list.get(i)).intValue() == 0) {
                q.offer(list.get(i));
            }
        }
        List<T> rtrn = new ArrayList<>(list.size());
        while(!q.isEmpty()){
            T t = q.poll();
            rtrn.add(t);
            List<T> deps = adjacencyMap.get(t);
            for (int i = 0, dSize = deps.size(); i < dSize; i++) {
                AtomicInteger degree = inDegrees.get(deps.get(i));
                if (degree != null && degree.decrementAndGet() == 0) {
                    q.offer(deps.get(i));
                }
            }
        }
        if (rtrn.size() != list.size()) {
            StringBuilder sb = new StringBuilder("Cycle detected in DAG involving: ");
            boolean first = true;
            for (int i = 0, size = list.size(); i < size; i++) {
                if (inDegrees.get(list.get(i)).get() > 0) {
                    if (!first) sb.append(", ");
                    sb.append(list.get(i));
                    first = false;
                }
            }
            throw new IllegalArgumentException(sb.toString());
        }
        Collections.reverse(rtrn);
        return rtrn;
    }

    public static <T> boolean isCircular(T item,Function<T,List<T>> getDependencies){
        if(item == null){
            return false;
        }
        List<T> dependencies = getDependencies.apply(item);
        if(dependencies.isEmpty()){
            return false;
        }
        Queue<T> q = new ArrayDeque<>(dependencies);
        T target;
        while((target=q.poll())!=null){
            if(item.equals(target)){
                return true;
            }
            q.addAll(getDependencies.apply(target));
        }
        return false;
    }
}
