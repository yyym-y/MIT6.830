package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class BufferHash {
    private final Die dd;
    public BufferHash(int numPage) {
        this.dd = new FIFO(numPage);
    }
    public Page getPage(PageId pid) {
        return dd.map.get(pid);
    }
    public void insert(PageId pid, Page pg) throws DbException {
        dd.insert(pid, pg);
    }
    public void del(PageId pid) {
        dd.del(pid);
    }
    public void flushPage(PageId pid) {
        dd.flushPage(pid);
    }
    public ConcurrentHashMap<PageId, Page> getMap() {
        return dd.map;
    }
    public void markDirty(PageId pid, boolean flag, TransactionId tid) {
        dd.map.get(pid).markDirty(flag, tid);
    }
    public boolean ifPageInBuffer(PageId pid) {
        return dd.map.get(pid) != null;
    }
    public void setBeforeImage(PageId pid) {
        dd.map.get(pid).setBeforeImage();
    }

    public abstract static class Die {
        public ConcurrentHashMap<PageId, Page> map = new ConcurrentHashMap<>();
        public final int numPages;
        public Die(int numPages) {this.numPages = numPages;}
        public void flushPage(PageId pid) {
            DbFile dbFile = Database.getCatalog().getDatabaseFile(pid.getTableId());
            try {
                TransactionId tid = map.get(pid).isDirty();
                if(tid != null) {
                    Database.getLogFile().logWrite(tid, map.get(tid).getBeforeImage(), map.get(pid));
                    Database.getCatalog().getDatabaseFile(pid.getTableId()).writePage(map.get(pid));
                }
                dbFile.writePage(map.get(pid));
                map.get(pid).setBeforeImage();
                map.remove(pid);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        public abstract void insert(PageId pid, Page pg) throws DbException;
        public abstract void del(PageId pid);
    }

    public static class FIFO extends Die {
        LinkedList<PageId> queue = new LinkedList<>();
        public FIFO(int numPages) {
            super(numPages);
        }
        @Override
        public void insert(PageId pid, Page pg) throws DbException {
            if(super.map.get(pid) != null) {
                super.map.put(pid, pg); return;
            }
            if(super.map.size() >= super.numPages) {
                boolean flag = false;
                for(PageId pr : queue) {
                    if(map.get(pr).isDirty() == null) {
                        flushPage(pr); flag = true;
                        queue.remove(pr); break;
                    }
                }
                if(! flag)
                    throw new DbException("do not have available clear page");
            }
            if(super.map.get(pid) == null)
                queue.add(pid);
            super.map.put(pid, pg);
        }
        @Override
        public void del(PageId pid) {
            map.remove(pid);
            queue.remove(pid);
        }
    }
}
