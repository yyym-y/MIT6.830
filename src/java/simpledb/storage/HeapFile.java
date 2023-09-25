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

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
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
         return pid.getPageNumber() * BufferPool.getPageSize();
     }


    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        return ((int) file.length() -  1) / BufferPool.getPageSize() + 1;
    }
    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs


    public DbFileIterator iterator(TransactionId tid) {
        return new DbFileIterator() {
            private Iterator<Tuple> item = null;
            private int nowPageNum = 0; // 最后一个没有读的Page编号
            private boolean opened = false;


            public void readItem() {
                if(nowPageNum > numPages() - 1) {
                    item = null; return;
                }
                HeapPageId hpid = new HeapPageId(getId(), nowPageNum);
                try {
                    HeapPage hpg = (HeapPage) Database.getBufferPool().getPage(tid, hpid, null);
                    item = hpg.iterator();
                    nowPageNum ++;
                } catch (TransactionAbortedException | DbException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public void open() throws DbException, TransactionAbortedException {
                rewind();
                opened = true;
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                if(item == null) return false;
                if(! item.hasNext()) {
                    readItem();
                    return item != null;
                }
                return true;
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                if(item == null || ! opened)
                    throw new NoSuchElementException();

                //System.out.println(item.hasNext());
                if(hasNext())
                    return item.next();
                throw new NoSuchElementException();
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                nowPageNum = 0;
                item = null;
                readItem();
            }

            @Override
            public void close() {
                opened = false;
            }
        };
    }
}

