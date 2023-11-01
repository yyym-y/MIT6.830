package simpledb.storage;

import com.sun.source.tree.ReturnTree;
import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.sql.ClientInfoStatus;
import java.util.*;

 /**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private final File file;
    private final TupleDesc td;

    private final int id;
    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        this.file = f; this.td = td;
        id = this.file.getAbsolutePath().hashCode();
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        return id;
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return td;
    }
    public static int IO = 0;

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        //System.out.println("IO num " + (++ HeapFile.IO));
        ++ HeapFile.IO;
        // 这里应该先在缓冲池里面寻找， 但是代码没写好，所以先直接在磁盘文件里查找
        if(pid.getPageNumber() > numPages()) {
            return null;
        }
        try(RandomAccessFile file = new RandomAccessFile(this.file, "r")) {
//            int each = BufferPool.getPageSize();
//            int begin = each * pid.getPageNumber();
//            byte[] info = new byte[each];
//            System.out.println(begin + "***");
//            System.out.println(this.file.length());
//            System.out.println(each + "^^^");
//            int num = bfInput.read(info, begin, each);
//            System.out.println(num);
//            HeapPage heapPage = new HeapPage((HeapPageId) pid, info);
//            return heapPage;
            long offset = getOffset(pid);
            byte[] data = new byte[BufferPool.getPageSize()];
            file.seek(offset);
            for (int i = 0, n = BufferPool.getPageSize(); i < n; ++i) {
                data[i] = file.readByte();
            }
            return new HeapPage((HeapPageId) pid, data);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

     private long getOffset(PageId pid) {
         return (long) pid.getPageNumber() * BufferPool.getPageSize();
     }


    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        try (RandomAccessFile file = new RandomAccessFile(this.file, "rw")) {
            long offset = getOffset(page.getId());
            byte[] bytes = page.getPageData();
            file.seek(offset);
            file.write(bytes);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        if(file.length() == 0) return 0;
        return ((int) file.length() -  1) / BufferPool.getPageSize() + 1;
    }

    public Page getPageFromBuffer(TransactionId tid, Tuple t, Permissions pid)
            throws TransactionAbortedException, DbException {
        return Database.getBufferPool().getPage(tid, t.getRecordId().getPageId(), pid);
    }

    public Page getPageFromBuffer(TransactionId tid, Tuple t)
            throws TransactionAbortedException, DbException {
        return Database.getBufferPool().getPage(tid, t.getRecordId().getPageId(), Permissions.READ_WRITE);
    }

    public Page getPageFromBuffer(TransactionId tid, int i)
            throws TransactionAbortedException, DbException {
        return Database.getBufferPool().getPage(tid, new HeapPageId(id, i), Permissions.READ_WRITE);
    }
    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        for(int i = 0 ; i < numPages() ; i++) {
            HeapPage hpg = (HeapPage) getPageFromBuffer(tid, i);
            try {
                hpg.insertTuple(t);
                hpg.markDirty(true, tid);
                return List.of(new Page[]{hpg});
            }catch (DbException ignored) {}
        }
        HeapPage hpg = new HeapPage(new HeapPageId(id, numPages()), new byte[BufferPool.getPageSize()]);
        t.setRecordId(new RecordId(new HeapPageId(id, numPages()), 0));
        hpg.markDirty(true, tid);
        hpg.insertTuple(t);
        writePage(hpg);
        return List.of(new Page[]{hpg});
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        HeapPage hpg = (HeapPage) getPageFromBuffer(tid, t);
        hpg.deleteTuple(t);
        hpg.markDirty(true, tid);
        Database.getBufferPool().resetPage(hpg);
        return new ArrayList<>(List.of(hpg));
    }

    // see DbFile.java for javadocs


    public DbFileIterator iterator(TransactionId tid) {
        return new DbFileIterator() {
            private boolean opened = false;


            ArrayList<Iterator<Tuple>> items = null;
            public void readAllTuples() throws TransactionAbortedException, DbException {
                items = new ArrayList<>();
                for(int i = 0 ; i < numPages() ; i ++) {
                    HeapPageId hpid = new HeapPageId(getId(), i);
                    HeapPage hpg = (HeapPage) Database.getBufferPool().getPage(tid, hpid, Permissions.READ_ONLY);
                    items.add(hpg.iterator());
                }
            }
            int pos = 0;
            @Override
            public void open() throws DbException, TransactionAbortedException {
                readAllTuples(); pos = 0;
                opened = true;
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                if(! opened || pos >= items.size())
                    return false;
                if(items.get(pos).hasNext()) return true;
                while ( pos < items.size() && ! items.get(pos).hasNext() )
                    pos ++;
                if(pos >= items.size())
                    return false;
                return items.get(pos).hasNext();
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                if(! opened)
                    throw new NoSuchElementException();
                if(hasNext())
                    return items.get(pos).next();
                throw new NoSuchElementException();
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                readAllTuples(); pos = 0;
            }

            @Override
            public void close() {
                opened = false;
            }
        };
    }
}

