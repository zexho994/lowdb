package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 *
 * @author Sam Madden
 * @see HeapPage#HeapPage
 */
public class HeapFile implements DbFile {

    private final File file;
    private final TupleDesc tupleDesc;
    private final int id;
    private int pageSize;


    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f the file that stores the on-disk backing store for this heap
     *          file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.id = f.getAbsolutePath().hashCode();
        this.file = f;
        this.tupleDesc = td;
        this.pageSize = (int) (this.file.length() / BufferPool.getPageSize());
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
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
        // some code goes here
        return this.id;
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.tupleDesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
            long pos = (long) pid.getPageNumber() * BufferPool.getPageSize();
            raf.seek(pos);
            byte[] bytes = new byte[BufferPool.getPageSize()];
            raf.read(bytes, 0, BufferPool.getPageSize());
            return new HeapPage((HeapPageId) pid, bytes);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        int pageNumber = page.getId().getPageNumber();
        byte[] data = page.getPageData();
        try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
            int pos = pageNumber * BufferPool.getPageSize();
            raf.seek(pos);
            raf.write(data);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return this.pageSize;
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t) throws DbException, IOException, TransactionAbortedException {
        // some code goes here

        HeapPage page = null;
        // 查找可用的page
        for (int i = 0; i < numPages(); i++) {
            page = (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(id, i), Permissions.READ_WRITE);
            if (page.getNumEmptySlots() > 0) {
                break;
            }
            page = null;
        }

        if (page == null) {
            page = new HeapPage(new HeapPageId(id, numPages()), new byte[BufferPool.getPageSize()]);
            pageSize++;
        }
        //进行插入操作，标记page为dirty
        page.insertTuple(t);
        page.markDirty(true, tid);
        return Collections.singletonList(page);
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException, TransactionAbortedException {
        // some code goes here
        throw new RuntimeException("");
//        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new DbFileIterator() {
            int idx = 0;
            Iterator<Tuple> iterator;

            @Override
            public void open() throws DbException, TransactionAbortedException {
                idx = 0;
                iterator = getIterator(idx);
            }

            private Iterator<Tuple> getIterator(int pageNo) throws TransactionAbortedException, DbException {
                if (pageNo > numPages()) {
                    return null;
                }
                HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(id, pageNo), Permissions.READ_WRITE);
                iterator = page.iterator();
                return iterator;
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                if (iterator == null) {
                    return false;
                } else if (iterator.hasNext()) {
                    return true;
                }
                iterator = getIterator(++idx);
                if (iterator == null) {
                    return false;
                }
                return this.hasNext();
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                if (iterator == null) {
                    throw new NoSuchElementException("Iterator iterator not open yet.");
                }
                return iterator.next();
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                close();
                open();
            }

            @Override
            public void close() {
                iterator = null;
                idx = 0;
            }

        };
    }

}

