package simpledb.storage;

import simpledb.common.Database;

import java.io.IOException;
import java.util.LinkedList;
import java.util.concurrent.ConcurrentHashMap;

public class BufferHash {
    private final Die dd;
    public BufferHash(int numPage) {
        this.dd = new FIFO(numPage);
    }
    public Page getPage(PageId pid) {
        return dd.map.get(pid);
    }
    public void insert(PageId pid, Page pg) {
        dd.insert(pid, pg);
    }
    public void del(PageId pid) {
        dd.del(pid);
    }
    public void flushPage(PageId pid) {
        dd.flushPage(pid);
    }

    public abstract static class Die {
        public ConcurrentHashMap<PageId, Page> map = new ConcurrentHashMap<>();
        public final int numPages;
        public Die(int numPages) {this.numPages = numPages;}
        public void flushPage(PageId pid) {
            DbFile dbFile = Database.getCatalog().getDatabaseFile(pid.getTableId());
            try {
                dbFile.writePage(map.get(pid));
                map.remove(pid);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        public abstract void insert(PageId pid, Page pg);
        public abstract void del(PageId pid);
    }

    public static class FIFO extends Die {
        LinkedList<PageId> queue = new LinkedList<>();
        public FIFO(int numPages) {
            super(numPages);
        }
        @Override
        public void insert(PageId pid, Page pg) {
            if(super.map.size() >= super.numPages) {
                PageId delId = queue.getFirst(); queue.removeFirst();
                super.flushPage(delId);
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
