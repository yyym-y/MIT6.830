package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.Permissions;
import simpledb.common.DbException;
import simpledb.common.DeadlockException;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    private final int numPages;
    private Get_pos getPos = new Get_pos();
    private int numSpace = 0;
    private ConcurrentHashMap<PageId, Page> map = new ConcurrentHashMap<>();
    private ConcurrentHashMap<PageId, Integer> pid_pos = new ConcurrentHashMap<>();
    private PageId[] slot = new PageId[DEFAULT_PAGES];
    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        this.numPages = numPages;
    }
    
    public static int getPageSize() {
      return pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
    	BufferPool.pageSize = pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
    	BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public  Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        Page pg = map.get(pid);
        if(pg != null) return pg;
        DbFile db = Database.getCatalog().getDatabaseFile(pid.getTableId());
        pg = db.readPage(pid);
        if(numSpace >= numPages){
            try {
                int num = getPos.getDel_pos();
                flushPage(slot[num]);
                numSpace--;
                map.remove(slot[num]);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        map.put(pid, pg);
        numSpace ++;
        try {
            int num = getPos.getIns_pos();
            slot[num] = pid;  pid_pos.put(pid, num);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return pg;
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public  void unsafeReleasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return false;
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit) {
        // some code goes here
        // not necessary for lab1|lab2
    }

    public void resetPage(Page p) {
        map.put(p.getId(), p);
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        DbFile dbFile = Database.getCatalog().getDatabaseFile(tableId);
        List<Page> pages = dbFile.insertTuple(tid, t);
        for(Page pr : pages) {
            resetPage(pr);
            map.put(t.getRecordId().getPageId(), pr);
            map.put(pr.getId(), pr);
        }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        DbFile dbFile = Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId());
        List<Page> pages = dbFile.deleteTuple(tid, t);
        for(Page pr : pages) {
            pr.markDirty(true, tid);
            resetPage(pr);
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1

    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
        
        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
        int pos = pid_pos.get(pid);
        map.remove(pid);
        getPos.del_pos(pos);
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        DbFile dbFile = Database.getCatalog().getDatabaseFile(pid.getTableId());
        dbFile.writePage(map.get(pid));
        map.remove(pid);
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1
    }


    public class Get_pos {

        private byte[] ifDel = new byte[(int) Math.ceil(DEFAULT_PAGES / 8.0)];
        private LinkedList<Integer> insert = new LinkedList<>();
        private LinkedList<Integer> delete = new LinkedList<>();

        public Get_pos() {
            for(int i = 0 ; i < DEFAULT_PAGES ; i++) {
                insert.add(i); setSlot(i, 1);
            }
        }
        public void setSlot(int pos, int value) {
            int num = pos / 8; pos %= 8;
            if(value == 1)
                ifDel[num] |= (1L << pos);
            else
                ifDel[num] ^= (ifDel[num] >> pos & 1 << pos);
        }
        public int getSlot(int pos) {
            int num = pos / 8; pos %= 8;
            return ifDel[num] >> pos & 1;
        }
        public int getIns_pos() throws Exception {
            if(insert.isEmpty())
                throw new Exception("no space to insert");
            int num = insert.remove();
            delete.add(num);
            return num;
        }

        public int getDel_pos() throws Exception {
            if(delete.isEmpty())
                throw new Exception("nothing to del");
            int num = delete.remove();
            while (getSlot(num) == 0) {
                if(delete.isEmpty())
                    throw new Exception("nothing to del");
                setSlot(num, 1);
                num = delete.remove();
            }
            insert.add(num);
            return num;
        }

        public void del_pos(int pos) {
            setSlot(pos, 0);
            insert.add(pos);
        }
    }

}
