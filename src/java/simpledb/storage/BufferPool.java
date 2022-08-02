package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.Permissions;
import simpledb.common.DbException;
import simpledb.index.BTreeFile;
import simpledb.index.BTreeLeafPage;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

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

    /**
     * Default number of pages passed to the constructor. This is used by
     * other classes. BufferPool should use the numPages argument to the
     * constructor instead.
     */
    public static final int DEFAULT_PAGES = 50;

    private final Map<PageId, Page> pages;
    private final PageLock pageLock;
    private final int maxPages;

    static class PageLock {

        private final Map<PageId, Holders> pageHolderCache = new ConcurrentHashMap<>();
        private final String innerLock = "";

        /**
         * 获取锁
         * 1. pageId 没有 holder，可以获取锁
         * 2. pageId 有 holder，但是holder是自己,可以获取锁
         * 3. 其他人获取了锁，不能获取锁
         */
        public boolean lock(PageId pid, TransactionId tid, Permissions perm) {

            synchronized (innerLock) {
                Holders holders = pageHolderCache.get(pid);
                // 没有锁
                if (holders == null || holders.size() == 0) {
                    holders = new Holders();
                    pageHolderCache.put(pid, holders);
                    holders.addHolder(Holder.build(tid, perm));
                    return true;
                } else if (holders.size() == 1 && holders.get(tid) != null) {
                    // 有一个锁,且自己的锁: 升级 or 降级
                    holders.get(tid).reentrant(perm);
                    return true;
                } else if (perm == Permissions.READ_ONLY && holders.map.values().stream().allMatch(l -> l.exclusive.get() == 0)) {
                    // 都是读锁,可以共享
                    holders.addHolder(Holder.build(tid, Permissions.READ_ONLY));
                    return true;
                }
            }

            return false;
        }

        /**
         * 释放锁
         */
        public void unlock(PageId pid, TransactionId tid) {
            synchronized (innerLock) {
                Holder holder = pageHolderCache.get(pid).get(tid);
                int count = 0;
                if (holder.share.get() > 0) {
                    count = holder.share.decrementAndGet();
                } else {
                    count = holder.exclusive.decrementAndGet();
                }
                if (count == 0) {
                    pageHolderCache.get(pid).map.remove(tid);
                }
            }
        }

        public void unlockAll(PageId pid) {
            synchronized (innerLock) {
                pageHolderCache.remove(pid);
            }
        }

        public boolean hold(PageId pid, TransactionId tid) {
            return pageHolderCache.get(pid).contains(tid);
        }

        public boolean hasLock(PageId pid) {
            Holders l = pageHolderCache.get(pid);
            return l != null && l.size() > 0;
        }

    }

    static class Holders {
        private final Map<TransactionId, Holder> map = new ConcurrentHashMap<>();

        public void addHolder(Holder holder) {
            this.map.put(holder.tid, holder);
        }

        public boolean contains(TransactionId t) {
            return map.containsKey(t);
        }

        public Holder get(TransactionId tid) {
            return map.get(tid);
        }

        public void remove(TransactionId tid) {
            map.remove(tid);
        }

        public int size() {
            return map.size();
        }

    }

    static class Holder {
        private final TransactionId tid;
        private final AtomicInteger share = new AtomicInteger(0);
        private final AtomicInteger exclusive = new AtomicInteger(0);

        public Holder(TransactionId tid, int share, int exclusive) {
            this.tid = tid;
            this.share.set(share);
            this.exclusive.set(exclusive);
        }

        public static Holder build(TransactionId tid, Permissions perm) {
            if (perm == Permissions.READ_ONLY) {
                return new Holder(tid, 1, 0);
            } else {
                return new Holder(tid, 0, 1);
            }
        }

        public boolean release() {
            if (share.get() > 0) {
                return share.decrementAndGet() == 0;
            } else {
                return exclusive.decrementAndGet() == 0;
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Holder)) return false;
            Holder holder = (Holder) o;
            return tid.equals(holder.tid);
        }

        @Override
        public int hashCode() {
            return Objects.hash(tid);
        }

        public void reentrant(Permissions perm) {
            if (perm == Permissions.READ_ONLY) {
                if (exclusive.get() > 0) {
                    share.set(exclusive.incrementAndGet());
                    exclusive.set(0);
                } else {
                    share.incrementAndGet();
                }
            } else {
                if (exclusive.get() > 0) {
                    exclusive.incrementAndGet();
                } else {
                    exclusive.set(share.incrementAndGet());
                    share.set(0);
                }
            }
        }

    }

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        this.pages = new ConcurrentHashMap<>(numPages);
        this.maxPages = numPages;
        this.pageLock = new PageLock();
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
     * @param tid  the ID of the transaction requesting the page
     * @param pid  the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm) throws TransactionAbortedException, DbException {
        // some code goes here
        long startTime = System.currentTimeMillis();
        while (!pageLock.lock(pid, tid, perm)) {
            if (System.currentTimeMillis() - startTime > 3000) {
                throw new TransactionAbortedException();
            }
        }
        Page page = pages.get(pid);
        if (page == null) {
            DbFile databaseFile = Database.getCatalog().getDatabaseFile(pid.getTableId());
            if (databaseFile == null) {
                return null;
            }
            if (this.numPages() >= maxPages()) {
                this.evictPage();
            }
            page = databaseFile.readPage(pid);
            pages.put(pid, page);
            // 返回page前先记录下快照
            page.setBeforeImage();
        }

        return page;
    }

    public void unlock(PageId pid, TransactionId tid) {
        pageLock.unlock(pid, tid);
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
    public void unsafeReleasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
        pageLock.unlock(pid, tid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) {
        // some code goes here
        // not necessary for lab1|lab2
        this.transactionComplete(tid, true);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid    the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit) {
        // some code goes here
        // not necessary for lab1|lab2
        Iterator<PageId> iterator = this.pages.keySet().stream().iterator();
        // 遍历所有page
        while (iterator.hasNext()) {
            // 找到tid包含的事务
            try {
                PageId pid = iterator.next();
                if (commit) {
                    flushPages(tid);
                    Database.getLogFile().logCheckpoint();
                } else {
                    recoverPages(tid);
                }
                if (this.pageLock.hasLock(pid)) {
                    this.pageLock.pageHolderCache.get(pid).remove(tid);
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    // 恢复page的数据，使用before image覆盖当前数据
    public void recoverPages(TransactionId tid) {
        try {
            Database.getLogFile().rollback(tid);
            Database.getLogFile().logCheckpoint();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return pageLock.hold(p, tid);
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other
     * pages that are updated (Lock acquisition is not needed for lab2).
     * May block if the lock(s) cannot be acquired.
     * <p>
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid     the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t       the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t) throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        DbFile databaseFile = Database.getCatalog().getDatabaseFile(tableId);
        databaseFile.insertTuple(tid, t);
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     * <p>
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid the transaction deleting the tuple.
     * @param t   the tuple to delete
     */
    public void deleteTuple(TransactionId tid, Tuple t) throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        BTreeLeafPage page = (BTreeLeafPage) getPage(tid, t.getRecordId().getPageId(), Permissions.READ_WRITE);
        if (page == null) {
            throw new DbException("page not exist");
        }
        BTreeFile databaseFile = (BTreeFile) Database.getCatalog().getDatabaseFile(page.getId().getTableId());
        databaseFile.deleteTuple(tid, t);
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     * break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1
        for (Map.Entry<PageId, Page> entry : pages.entrySet()) {
            Page page = entry.getValue();
            // 只对脏页进行刷新
            if (page.isDirty() != null) {
                flushPage(page.getId());
            }
        }
    }

    /**
     * Remove the specific page id from the buffer pool.
     * Needed by the recovery manager to ensure that the
     * buffer pool doesn't keep a rolled back page in its
     * cache.
     * <p>
     * Also used by B+ tree files to ensure that deleted pages
     * are removed from the cache so they can be reused safely
     */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // not necessary for lab1
        this.pages.remove(pid);
        this.pageLock.unlockAll(pid);
    }

    /**
     * Flushes a certain page to disk
     *
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1
        Page page = pages.get(pid);
        DbFile file = Database.getCatalog().getDatabaseFile(pid.getTableId());

        // 保存脏页再刷盘
        if (page.isDirty() != null) {
            Database.getLogFile().logWrite(page.isDirty(), page.getBeforeImage(), page);
            Database.getLogFile().force();
        }

        file.writePage(page);
        page.markDirty(false, null);
    }

    /**
     * Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        for (Map.Entry<PageId, Page> entry : pages.entrySet()) {
            Page page = entry.getValue();
            if (page.isDirty() == tid) {
                flushPage(page.getId());
                page.setBeforeImage();
            }
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1

        Iterator<Map.Entry<PageId, Page>> iterator = pages.entrySet().iterator();
        //遍历所有页面
        while (iterator.hasNext()) {
            Map.Entry<PageId, Page> next = iterator.next();
            PageId pid = next.getKey();
            if (Database.getBufferPool().pages.get(pid).isDirty() == null) {
                //替换掉干净的页面
                try {
                    flushPage(pid);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                discardPage(pid);
                return;
            }
        }
        throw new DbException("");
    }

    public int numPages() {
        return pages.size();
    }

    public int maxPages() {
        return maxPages;
    }

}
