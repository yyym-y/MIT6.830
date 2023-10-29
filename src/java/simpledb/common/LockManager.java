package simpledb.common;

import com.sun.security.jgss.GSSUtil;
import simpledb.storage.Page;
import simpledb.storage.PageId;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class LockManager {
    public static class PageLock {
        private PageId pid; // 记录这个页锁锁住的 Page 的 id
        private Permissions lockType; // 记录锁住这个页的锁的我类型，记录是读锁还是写锁
        public PageLock(PageId pid, Permissions lockType) {
            this.pid = pid;
            this.lockType = lockType;
        }
        public PageId getPid() {
            return pid;
        }
        public Permissions getLockType() {
            return lockType;
        }
        public void setLockType(Permissions prem) {
            this.lockType = prem;
        }
        @Override
        public int hashCode() {
            // 注意： 哈希值只是用了 pid 的哈希值，没有使用锁类型的哈希值
            return pid.hashCode();
        }
        @Override
        public boolean equals(Object o) {
            // 比较也只是比较 pid， 没有比较锁类型
            if(o == null) return false;
            if(o.getClass() != getClass()) return false;
            PageLock other = (PageLock) o;
            return pid.equals(other.getPid());
        }

        @Override
        public String toString() {
            return "PageLock{" +
                    "pid=" + pid +
                    ", lockType=" + lockType +
                    '}';
        }
    }
    public static class DirectedGraph {
        ConcurrentHashMap<TransactionId, HashSet<TransactionId>> graph = new ConcurrentHashMap<>();
        public synchronized void addNode(TransactionId tid) {
            if(graph.containsKey(tid)) return;
            graph.put(tid, new HashSet<>());
        }
        public synchronized void addEdge(TransactionId tid1, TransactionId tid2) {
            addNode(tid1); addNode(tid2);
            graph.get(tid1).add(tid2);
        }
        public synchronized void removeNode(TransactionId tid) {
            for(TransactionId ttid : graph.keySet()) {
                graph.get(ttid).remove(tid);
            }
            graph.remove(tid);
        }
        public synchronized void removeEdge(TransactionId tid1, TransactionId tid2) {
            if(graph.containsKey(tid1))
                graph.get(tid1).remove(tid2);
            if(graph.get(tid1).isEmpty())
                graph.remove(tid1);
        }
        volatile HashSet<TransactionId> check;
        public synchronized boolean dfs(TransactionId i) {
            if(graph.get(i) == null) return false;
            for(TransactionId pr : graph.get(i)) {
                if(check.contains(pr))
                    return true;
                check.add(pr);
                if(dfs(pr))
                    return true;
            }
            return false;
        }
        // 判断是否有环
        public synchronized boolean ifCircle() {
            check = new HashSet<>();
            if(graph.size() <= 1) return false;
            TransactionId tid = null;
            for(TransactionId pr : graph.keySet()) {
                tid = pr; break;
            }
            check.add(tid);
            return dfs(tid);
        }
        public synchronized void showGraph() {
            for(TransactionId tid : graph.keySet()) {
                for(TransactionId tid2 : graph.get(tid))
                    System.out.println(tid + "---->" + tid2);
            }
        }
    }
    // 获取这个 Page 对应的所有没有提交的事务 ID
    private final ConcurrentHashMap<PageId, HashSet<TransactionId>> pageIdToTran = new ConcurrentHashMap<>();
    // 获取这个事务曾经获得过的所有页级锁
    private final ConcurrentHashMap<TransactionId, HashSet<PageLock>> tranToPageId = new ConcurrentHashMap<>();
    // 获取这个页当前被几个页面拥有了读锁
    volatile ConcurrentHashMap<PageId, Integer> readNum = new ConcurrentHashMap<>();
    // 判断这个页是否被某个页面拥有了写锁
    volatile ConcurrentHashMap<PageId, Boolean> ifWrite = new ConcurrentHashMap<>();
    volatile DirectedGraph g = new DirectedGraph();

    // ---------------------- It's all tool functions below -------------------------------------------//

    public synchronized void addRead(PageId pid) {
        readNum.merge(pid, 1, Integer::sum);
    }
    public synchronized void delRead(PageId pid) {
        readNum.merge(pid, -1, Integer::sum);
    }
    public synchronized Integer getReadNum(PageId pid) {
        if(readNum.get(pid) == null) return 0;
        return readNum.get(pid);
    }
    public synchronized void setWrite(PageId pid) {
        ifWrite.put(pid, true);
    }
    public synchronized void setUnWrite(PageId pid) {
        ifWrite.put(pid, false);
    }
    public synchronized Boolean getIfWrite(PageId pid) {
        if(ifWrite.get(pid) == null) return false;
        return ifWrite.get(pid);
    }

    // ---------------------- It's all add lock functions below -------------------------------------------//

    public synchronized void addLock(PageId pid, TransactionId tid, Permissions prem) throws TransactionAbortedException {
        HashSet<TransactionId> pageOfTran = pageIdToTran.get(pid); // 这个页面现在被哪些事务拿着锁
        // 如果这个页面当前没有给任何事务拿到任何锁
        if(pageOfTran == null) {
            notLockBefore(pid, tid, prem);
        } else {
            // 我想申请读锁， 但这个页面被某些事务拿到了某些锁
            if(prem.equals(Permissions.READ_ONLY)) {
                addReadLock(pid, tid);
            } else {
            // 我想申请写锁， 但这个页面被某些事务拿到了某些锁
                addWriteLock(pid, tid);
            }
        }
    }
    public synchronized void notLockBefore(PageId pid, TransactionId tid, Permissions prem) {
        // 记录这个页已经被这个事务拿到锁了
        pageIdToTran.put(pid, new HashSet<>(List.of(new TransactionId[]{tid})));
        // 记录这个事务已经拿到了这个页的锁
        if(tranToPageId.get(tid) == null)
            tranToPageId.put(tid, new HashSet<>(List.of(new PageLock[] {new PageLock(pid, prem)})));
        else
            tranToPageId.get(tid).add(new PageLock(pid, prem));
        if(prem.equals(Permissions.READ_WRITE))
            setWrite(pid);
        else addRead(pid);
    }
    public synchronized void addReadLock(PageId pid, TransactionId tid) throws TransactionAbortedException {
        HashSet<TransactionId> pageOfTran = pageIdToTran.get(pid); // 这个页面现在被哪些事务拿着锁
        if(pageOfTran.contains(tid)) // 如果这个页面被这个事务已经拿了锁了， 直接返回
            return;
        // 否则，就需要判断是否等其他事务的锁释放，值得注意的是，如果申请的是读锁，那么需要等写锁释放
        TransactionId first_tid = pageIdToTran.get(pid).iterator().next();
        if(getIfWrite(pid))
            g.addEdge(first_tid, tid);
        if(g.ifCircle()) {
            // 如果有环产生， 那么代表有死锁
            if(getIfWrite(pid))
                g.removeEdge(first_tid, tid);
            throw new TransactionAbortedException();
        }
        while (getIfWrite(pid)) {
            try {
                this.wait();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        addRead(pid);
        pageIdToTran.computeIfAbsent(pid, k -> new HashSet<>());
        pageIdToTran.get(pid).add(tid);
        tranToPageId.computeIfAbsent(tid, k -> new HashSet<>());
        tranToPageId.get(tid).add(new PageLock(pid, Permissions.READ_ONLY));
    }
    public synchronized void updateReadToWrite(PageId pid, TransactionId tid) throws TransactionAbortedException {
        for(PageLock plk : tranToPageId.get(tid)) {
            if(plk.getPid().equals(pid) && plk.getLockType().equals(Permissions.READ_ONLY)) {
                tranToPageId.get(tid).remove(new PageLock(pid, Permissions.READ_ONLY));
                tranToPageId.get(tid).add(new PageLock(pid, Permissions.READ_WRITE));
                delRead(pid); setWrite(pid);
                return;
            }
        }
        throw new TransactionAbortedException();
    }
    public synchronized void addWriteLockButUpdate(PageId pid, TransactionId tid) throws TransactionAbortedException {
        HashSet<TransactionId> pageOfTran = pageIdToTran.get(pid); // 这个页面现在被哪些事务拿着锁
        HashSet<PageLock> tranOfPage = tranToPageId.get(tid); // 这个事务现在拿着哪些页的锁
        // 刚好我自己拿着写锁
        if(getIfWrite(pid) && getReadNum(pid).equals(0))
            return;
        // 只有我一个人拿着读锁
        if(getIfWrite(pid).equals(Boolean.FALSE) && getReadNum(pid).equals(1)) {
            updateReadToWrite(pid, tid); return;
        }
        // 不止我一个人拿着读锁
        for(TransactionId first_tid : pageOfTran) {
            // 要等待除了我自己的其他事务完成
            if(first_tid.equals(tid)) continue;
            g.addEdge(first_tid, tid);
        }
        if(g.ifCircle()) {
            for(TransactionId first_tid : pageOfTran) {
                if(first_tid.equals(tid)) continue;
                g.removeEdge(first_tid, tid);
            }
            throw new TransactionAbortedException();
        }
        while (! getReadNum(pid).equals(1)) {
            try {
                this.wait();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        updateReadToWrite(pid, tid);
        setWrite(pid);
    }
    public synchronized void addWriteLockButNone(PageId pid, TransactionId tid) throws TransactionAbortedException {
        HashSet<TransactionId> pageOfTran = pageIdToTran.get(pid); // 这个页面现在被哪些事务拿着锁
        for(TransactionId first_tid : pageOfTran) {
            g.addEdge(first_tid, tid);
        }
        if(g.ifCircle()) {
            for(TransactionId first_tid : pageOfTran) {
                g.removeEdge(first_tid, tid);
            }
            throw new TransactionAbortedException();
        }
        if(getReadNum(pid) !=  null && ! getReadNum(pid).equals(0)) {
            try {
                this.wait();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        setWrite(pid);
        pageIdToTran.computeIfAbsent(pid, k -> new HashSet<>());
        pageIdToTran.get(pid).add(tid);
        tranToPageId.computeIfAbsent(tid, k -> new HashSet<>());
        tranToPageId.get(tid).add(new PageLock(pid, Permissions.READ_WRITE));
    }
    public synchronized void addWriteLock(PageId pid, TransactionId tid) throws TransactionAbortedException {
        HashSet<TransactionId> pageOfTran = pageIdToTran.get(pid); // 这个页面现在被哪些事务拿着锁
        if(pageOfTran.contains(tid)) {
            addWriteLockButUpdate(pid, tid);
        } else {
            addWriteLockButNone(pid, tid);
        }
    }

    // ---------------------- It's all release lock functions below -------------------------------------------//

    public synchronized void releaseTidLock(TransactionId tid) {
        HashSet<PageLock> tranOfPage = tranToPageId.get(tid); // 这个事务现在拿着哪些页的锁
        for(PageLock plk : tranOfPage) {
            if(plk.getLockType().equals(Permissions.READ_ONLY)) {
                delRead(plk.getPid());
            }else
                setUnWrite(plk.getPid());
            pageIdToTran.get(plk.getPid()).remove(tid);
            if(pageIdToTran.get(plk.getPid()).isEmpty())
                pageIdToTran.remove(plk.getPid());
        }
        g.removeNode(tid);
        tranToPageId.remove(tid);
        notifyAll();
    }
    public synchronized void releaseExactLock(TransactionId tid, PageId pid) {
        // 这里没有对计数器递减， 因为操作本身就是不安全的
        pageIdToTran.get(pid).remove(tid);
        tranToPageId.get(tid).remove(new PageLock(pid, null));
        notifyAll();
    }
    public synchronized void releaseAllLock() {
        pageIdToTran.clear(); tranToPageId.clear();
        readNum.clear(); ifWrite.clear(); g = new DirectedGraph();
        notifyAll();
    }

    // ---------------------- It's all print infos functions below -------------------------------------------//

    public synchronized Set<PageId> getTidPage(TransactionId tid) {
        if (tranToPageId.get(tid) == null) return null;
        return tranToPageId.get(tid).stream()
                                    .map(PageLock::getPid)
                                    .collect(Collectors.toSet());
    }
    public synchronized Set<Permissions> getPagePer(PageId pid) {
        Set<Permissions> set = new HashSet<>();
        for(TransactionId tid : pageIdToTran.get(pid)) {
            for(PageLock plk : tranToPageId.get(tid))
                set.add(plk.getLockType());
        }
        return set;
    }
    public synchronized void getAllLock() {
        for(PageId pid : pageIdToTran.keySet()) {
            System.out.println(pid + " " + getPagePer(pid));
        }
    }
    public synchronized void getAllTidLock() {
        for(TransactionId tid : tranToPageId.keySet()) {
            System.out.println(tid + " " + tranToPageId.get(tid));
        }
    }
    public synchronized void getAllReadNum() {
        for(PageId pid : pageIdToTran.keySet()) {
            System.out.println(pid + " " + getReadNum(pid));
        }
    }
    public synchronized void getAllWriteNum() {
        for(PageId pid : pageIdToTran.keySet()) {
            System.out.println(pid + " " + getIfWrite(pid));
        }
    }
    public synchronized void printAllInfo() {
        System.out.println("ALL Lock :");
        getAllLock();
        System.out.println("ALL Lock With Tid :");
        getAllTidLock();
        System.out.println("ALL Read Info:");
        getAllReadNum();
        System.out.println("ALL Write Info:");
        getAllWriteNum();
        g.showGraph();
    }
    public synchronized void getWaitInfo(PageId pid) {
        System.out.println(pid + " " + getReadNum(pid) + " " + getIfWrite(pid));
        for(TransactionId Tid : pageIdToTran.get(pid)) {
            for(PageLock plk : tranToPageId.get(Tid)) {
                if(plk.getPid().equals(pid))
                    System.out.println(plk);
            }
        }
        System.out.println("-------------------------------");
    }
}
