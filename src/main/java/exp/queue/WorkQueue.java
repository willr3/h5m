package exp.queue;

import exp.entity.Node;
import exp.entity.Value;
import exp.entity.Work;
import exp.svc.NodeGroupService;
import exp.svc.NodeService;
import exp.svc.ValueService;
import exp.svc.WorkService;
import io.vertx.core.impl.ConcurrentHashSet;
import jakarta.transaction.Transactional;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;


/*
 * Ideas:
 * * change to accept Runnable and keep the runnable behind the preceding Work when re-sorting
 *   This would allow us to have "callbacks" in the work queue that do not persist to DB
 *   For Example: having a synchronous upload that responds with change detection when all nodes finish calculating
*
 */
public class WorkQueue implements BlockingQueue<Runnable> {

    //TODO using counters blocks work on different values, change to set of active work
    //private Counters<Node> counters = new Counters<>();
    private Set<Work> activeWork = new ConcurrentHashSet<>();

    private final AtomicInteger count = new AtomicInteger();
    private final ReentrantLock takeLock = new ReentrantLock();
    private final Condition notEmpty = takeLock.newCondition();
    private final ReentrantLock putLock = new ReentrantLock();

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
        activeWork.remove(work);
        takeLock.lock();
        try {
            if(!runnables.isEmpty()){
                //signal all because this could unblock multiple work items
                notEmpty.signalAll();
            }
        } finally {
            takeLock.unlock();
        }
    }

    private void fullyLock(){
        takeLock.lock();
        putLock.lock();
    }
    private void fullyUnlock(){
        takeLock.unlock();
        putLock.unlock();
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
            }
        }finally {
            fullyUnlock();
        }
        if(found !=null && found instanceof WorkRunner){
            activeWork.add(((WorkRunner) found).work);
        }
        return found;
    }

    /**
     * get the runables that the input runnale must follow
     * @param runnable
     * @return
     */
    public List<Runnable> getRequiredPrecedingRunables(Runnable runnable){
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
        runnables = KahnDagSort.sort(runnables,this::getRequiredPrecedingRunables);
    }
    public void addWorks(Collection<Work> works){
        putLock.lock();
        try {
            works.forEach(w->runnables.add(new WorkRunner(w,this,nodeService,valueService,workService)));
            int c = count.getAndAdd(works.size());
            sort();
            if(c == 0){
                signalNotEmpty();
            }
        }finally {
            putLock.unlock();
        }
    }
    public void addWork(Work work) {
        add(new WorkRunner(work,this,nodeService,valueService,workService));
    }
    @Override
    public boolean add(Runnable runnable) {
        putLock.lock();
        try {
            runnables.add(runnable);
            int c = count.getAndIncrement();
            if(runnable instanceof WorkRunner){
                sort();
            }
            if(c == 0){
                signalNotEmpty();
            }
        }finally {
            putLock.unlock();
        }
        //This is not supported
        return true;
    }

    public boolean hasWork(Work work){
        return runnables.stream().anyMatch(v->v instanceof WorkRunner && work.equals(((WorkRunner) v).work));
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
        if(count.get()==0){
            return null;
        }
        takeLock.lock();
        try{
            if(count.get()==0){
                return null;
            }
            Runnable found = removeFirstUnblocked();
            if(found == null){
                return null;
            }
            int c = count.getAndDecrement();
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
        if(count.get()==0){
            return null;
        }
        takeLock.lock();
        try{
            return count.get() > 0 ? runnables.get(0) : null;
        }finally {
            takeLock.unlock();
        }
    }

    @Override
    public void put(Runnable runnable) throws InterruptedException {
        putLock.lock();
        try{
            runnables.add(runnable);
            int c = count.incrementAndGet();
            if(c ==0){
                signalNotEmpty();
            }
        }finally {
            putLock.unlock();
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
            while (count.get() == 0) {
                notEmpty.await();
            }
            Runnable work = null;
            do {
                work = removeFirstUnblocked();
                if (work == null) {
                    notEmpty.await();
                }
            } while (work == null);
            int c = count.getAndDecrement();
            if (c > 1) {
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
            while(count.get()==0){
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
            int c = count.getAndDecrement();
            if( c > 1){
                notEmpty.signal();
            }
            return work;
        } finally {
            takeLock.unlock();
        }
    }

    @Override
    public int remainingCapacity() {
        return Integer.MAX_VALUE - count.get();
    }

    @Override
    public boolean remove(Object o) {
        boolean rtrn = false;
        fullyLock();
        try{
            rtrn = runnables.remove(o);
            int c = count.getAndDecrement();
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
            runnables.addAll(c);
            count.getAndAdd(c.size());
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
            count.getAndAdd(c.size());
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




