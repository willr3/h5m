package io.hyperfoil.tools.h5m.queue;

import io.hyperfoil.tools.h5m.entity.Work;
import io.hyperfoil.tools.h5m.svc.NodeService;
import io.hyperfoil.tools.h5m.svc.ValueService;
import io.hyperfoil.tools.h5m.svc.WorkService;
import io.vertx.core.impl.ConcurrentHashSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;


/*
 * Ideas:
 * * change to accept Runnable and keep the runnable behind the preceding Work when re-sorting
 *   This would allow us to have "callbacks" in the work queue that do not persist to DB
 *   For Example: having a synchronous upload that responds with change detection when all nodes finish calculating
*
 */
public class WorkQueue implements BlockingQueue<Runnable> {

    private static final Logger log = LoggerFactory.getLogger(WorkQueue.class);
    //TODO using counters blocks work on different values, change to set of active work
    //private Counters<Node> counters = new Counters<>();
    private Set<Work> activeWork = new ConcurrentHashSet<>();
    private Set<Work> pendingWork = new ConcurrentHashSet<>();

    private final ReentrantLock takeLock = new ReentrantLock();
    private final Condition notEmpty = takeLock.newCondition();
    //private final ReentrantLock putLock = new ReentrantLock();

    private List<Runnable> runnables = new  ArrayList<>();


    private NodeService nodeService;
    private WorkService workService;
    private ValueService valueService;

    public WorkQueue(NodeService nodeService, ValueService valueService, WorkService workService) {
        //counters.setCallback("onComplete",this::onComplete);

        this.nodeService = nodeService;
        this.valueService = valueService;
        this.workService = workService;

        //replace with a better performant option
        this.runnables = new CopyOnWriteArrayList<>();
    }

    public boolean isIdle(){
        return activeWork.isEmpty() && runnables.isEmpty();
    }

    void decrement(Work work){
        fullyLock();
        try {
            activeWork.remove(work); // will be re-added to the queue if needed
            if(!runnables.isEmpty()){
                //signal all because this could unblock multiple work items
                notEmpty.signalAll();
            }
        } finally {
            fullyUnlock();
        }
    }

    private void fullyLock(){
//        putLock.lock();
        takeLock.lock();


    }
    private void fullyUnlock(){
//        putLock.unlock();
        takeLock.unlock();
    }
    private void signalNotEmpty(){
        takeLock.lock();
        try{
            notEmpty.signal();
        }finally {
            takeLock.unlock();
        }
    }

    private int getScore(int index){
        Runnable runnable = runnables.get(index);
        if(runnable instanceof WorkRunner){
            WorkRunner workRunner = (WorkRunner) runnable;
            boolean blockedByActiveWork = activeWork.stream().anyMatch(w->workRunner.work.dependsOn(w));
            boolean blockedByPending = runnables.stream()
                    .filter(v->v instanceof WorkRunner)
                    .map(w->((WorkRunner) w).work)
                    .anyMatch(w->workRunner.work.dependsOn(w));
            if(blockedByActiveWork || blockedByPending){
                return 1;
            }
            //TODO this implementation will block work for different values using the same node
            return 0;
        }else if (index > 0){//if this a normable Runnable
            // can we just return index because it has to run after any preceding?
            // what if the preceding is already out of queue but not done?
            // we need some tracking between runnable when added
            return index;
        }else{
            return 0;
        }
    }

    private Runnable removeFirstUnblocked(){
        Runnable found = null;
        int idx = 0;
        int score = -1;

        fullyLock();
        try {
            if(runnables.isEmpty()){
                return null;
            }
            do {
                found = runnables.get(idx);
                score = getScore(idx);
                if (score > 0) {
                    found = null;
                }
                idx++;
            } while (idx < runnables.size() && score > 0);
            if (found != null) {
                Runnable removed = runnables.remove(idx - 1); //index of found
                assert !runnables.contains(found);
                if(found instanceof WorkRunner workRunner){
                    activeWork.add(workRunner.work);
                    pendingWork.remove(workRunner.work);
                }
            }
        }finally {
            fullyUnlock();
        }
        if(runnables.isEmpty()){
            synchronized (this) {
                this.notify();
            }
        }
        return found;
    }

    /**
     * get the runables that the input runnable must follow
     * @param runnable
     * @return
     */
    public List<Runnable> getRequiredPrecedingRunnables(Runnable runnable){
        int index = runnables.indexOf(runnable);
        if(runnable instanceof WorkRunner){
            WorkRunner workRunner = (WorkRunner) runnable;
            return runnables.stream().filter(v->
                    v instanceof WorkRunner && workRunner.work.dependsOn(((WorkRunner) v).work)
            ).toList();
        }else if(index > 0){
                return List.of(runnables.get(index-1));
        }else{
            return Collections.emptyList();
        }
    }
    private void sort(){
        runnables = KahnDagSort.sort(runnables,this::getRequiredPrecedingRunnables);
    }
    public void addWorks(Collection<Work> works){
//        putLock.lock();
        takeLock.lock();
        try {
            int c = runnables.size();
            works.forEach(w->{
                if(!isPending(w)){
                    pendingWork.add(w);
                    runnables.add(new WorkRunner(w, this, nodeService, valueService, workService));
                    assert isPending(w);
                }else{
                    workService.delete(w); // remove rejected
                }
            });
            sort();
            if(c == 0){
                signalNotEmpty();
            }
        } finally {
//            putLock.unlock();
            takeLock.unlock();
        }
    }
    public boolean addWork(Work work) {
        return add(new WorkRunner(work,this,nodeService,valueService,workService));
    }
    public boolean hasWork(Work work){
        return isPending(work) || isActive(work);
        //return runnables.stream().anyMatch(v->v instanceof WorkRunner && work.equals(((WorkRunner) v).work));
    }
    public boolean isPending(Work work){
        return pendingWork.contains(work);
    }
    public boolean isActive(Work work){
        return activeWork.contains(work);
    }

    public int pendingCount(){
        return pendingWork.size();
    }
    public int activeCount(){
        return activeWork.size();
    }

    @Override
    public boolean add(Runnable runnable) {
        if(runnable instanceof WorkRunner workRunner){
            //reject new work that is already pending
            //do NOT reject new work if it matches an active work because it could be a retry re-queue
            if(isPending(workRunner.work)){
                return false;//reject new work that is already pending
            }else {
                pendingWork.add(workRunner.work);
            }
        }
//        putLock.lock();
        takeLock.lock();
        try {
            int c = runnables.size();
            runnables.add(runnable);
            if(runnable instanceof WorkRunner){
                sort();
            }
            if(c == 0){
                signalNotEmpty();
            }
        }finally {
//            putLock.unlock();
            takeLock.unlock();
        }
        //This is not supported
        return true;
    }


    @Override
    public boolean offer(Runnable runnable) {
        //This is not supported
        return false;
    }

    @Override
    public Runnable remove() {
        Runnable r = poll();
        if(r == null){
            throw new NoSuchElementException();
        }
        return r;
    }

    @Override
    public Runnable poll() {
        if(runnables.size()==0){
            return null;
        }
        takeLock.lock();
        try{
            if(runnables.isEmpty()){
                return null;
            }
            Runnable found = removeFirstUnblocked();
            if(found == null){
                return null;
            }
            int c = runnables.size();
            if(c > 1){
                notEmpty.signal();
            }
            return found;
        } finally {
            takeLock.unlock();
        }
    }

    @Override
    public Runnable element() {
        Runnable rtrn = peek();
        if(rtrn==null){
            throw new NoSuchElementException();
        }
        return rtrn;
    }

    @Override
    public Runnable peek() {
        if(runnables.isEmpty()){
            return null;
        }
        takeLock.lock();
        try{
            return !runnables.isEmpty() ? runnables.getFirst() : null;
        }finally {
            takeLock.unlock();
        }
    }

    @Override
    public void put(Runnable runnable) throws InterruptedException {
//        putLock.lock();
        takeLock.lock();
        try{
            if(runnable instanceof WorkRunner workRunner){
                if(isPending(workRunner.work)){
                    return;
                }else {
                    pendingWork.add(workRunner.work);
                }
            }
            int c = runnables.size();
            runnables.add(runnable);
            if(c!=0){
                signalNotEmpty();
            }
        }finally {
//            putLock.unlock();
            takeLock.unlock();
        }
        //not supported
    }

    @Override
    public boolean offer(Runnable runnable, long timeout, TimeUnit unit) throws InterruptedException {
        //not supported
        return false;
    }

    @Override
    public Runnable take() throws InterruptedException {
        takeLock.lockInterruptibly();
        try {
            while (runnables.isEmpty()) {
                notEmpty.await();
            }
            Runnable work = null;
            do {
                work = removeFirstUnblocked();
                if (work == null) {
                    notEmpty.await();
                }
            } while (work == null);
            int c = runnables.size();
            if (c > 0) {
                notEmpty.signal();
            }
            return work;
        } finally {
            takeLock.unlock();
        }
    }

    @Override
    public Runnable poll(long timeout, TimeUnit unit) throws InterruptedException {
        long nanos = unit.toNanos(timeout);
        takeLock.lockInterruptibly();
        try {
            while(runnables.isEmpty()){
                if (nanos <= 0L){
                    return null;
                }
                nanos = notEmpty.awaitNanos(nanos);
            }
            Runnable work = null;
            while(work == null){
                work = removeFirstUnblocked();
                if(work == null){
                    if (nanos <= 0L){
                        return null;
                    }
                    nanos = notEmpty.awaitNanos(nanos);
                }
            }
            int c = runnables.size();
            if( c > 0){
                notEmpty.signal();
            }
            return work;
        } finally {
            takeLock.unlock();
        }
    }

    @Override
    public int remainingCapacity() {
        return Integer.MAX_VALUE;
    }

    @Override
    public boolean remove(Object o) {
        boolean rtrn = false;
        fullyLock();
        try{
            if(o instanceof WorkRunner workRunner){
                pendingWork.remove(workRunner.work);
            }
            rtrn = runnables.remove(o);
            int c = runnables.size();
            if(c>0){
                notEmpty.signal();
            }
        } finally {
            fullyUnlock();
        }
        return rtrn;
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        return runnables.containsAll(c);
    }

    @Override
    public boolean addAll(Collection<? extends Runnable> c) {
        fullyLock();
        try {
            for(Runnable r : runnables){
                if( r instanceof WorkRunner workRunner && isPending(workRunner.work)){
                    //do not add something already in queue
                }else{
                    if( r instanceof WorkRunner workRunner){
                        pendingWork.add(workRunner.work);
                    }
                    runnables.add(r);
                }
            }
            sort();
        }finally {
            fullyUnlock();
        }
        return false;
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        //not supported
        return false;
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        fullyLock();
        try {
            runnables.retainAll(c);
            sort();
        }finally {
            fullyUnlock();
        }
        return false;
    }

    @Override
    public void clear() {
        fullyLock();
        try {
            runnables.clear();
        }finally {
            fullyUnlock();
        }
    }

    @Override
    public int size() {
        return runnables.size();
    }

    @Override
    public boolean isEmpty() {
        return runnables.isEmpty();
    }

    @Override
    public boolean contains(Object o) {
        return runnables.contains(o);
    }

    @Override
    public Iterator<Runnable> iterator() {
        return runnables.iterator();
    }

    @Override
    public Object[] toArray() {
        return runnables.toArray();
    }

    @Override
    public <T> T[] toArray(T[] a) {
        return runnables.toArray(a);
    }

    @Override
    public int drainTo(Collection<? super Runnable> c) {
        //not supported
        return 0;
    }

    @Override
    public int drainTo(Collection<? super Runnable> c, int maxElements) {
        //not supported
        return 0;
    }
}




