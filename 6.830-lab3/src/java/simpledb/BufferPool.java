package simpledb;

import java.io.*;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
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
    public static final int PAGE_SIZE = 4096;

    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;
    
    private final int numPages;
    private final ConcurrentHashMap<Integer,Page> pageStore;
    
    private final LockManager lockManager;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
    	this.numPages = numPages;
    	pageStore = new ConcurrentHashMap<Integer,Page>();
    	lockManager = new LockManager();
    }
    
    public static int getPageSize() {
      return PAGE_SIZE;
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
    	// must lock before usage
    	lockManager.lock(tid, pid, perm);
    	
        // some code goes here
    	if(!pageStore.containsKey(pid.hashCode())){
    		DbFile dbfile = Database.getCatalog().getDatabaseFile(pid.getTableId());
    		Page page = dbfile.readPage(pid);
    		if(pageStore.size() >= numPages){
    			evictPage();
    		}
    		pageStore.put(pid.hashCode(), page);
    	}
    	
    	return pageStore.get(pid.hashCode());
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
    public  void releasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
    	// must lock before usage
    	lockManager.unlock(tid, pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    	transactionComplete(tid,true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return lockManager.holdsLock(tid, p);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
        throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    	if(commit){
    		// flush all pages dirtied by this transaction
    		flushPages(tid);
    	}else{
    		// restore all pages dirtied by this transaction
    		Set<PageId> pids = lockManager.getDirtyPageIds(tid);
        	for(PageId pid: pids){
        		pageStore.remove(pid.hashCode());
        		try {
					getPage(tid,pid, Permissions.READ_ONLY);
				} catch (TransactionAbortedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (DbException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
        	}
    	}
    	
    	lockManager.unlock(tid);
    }

    /**
     * Add a tuple to the specified table behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to(Lock 
     * acquisition is not needed for lab2). May block if the lock cannot 
     * be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and updates cached versions of any pages that have 
     * been dirtied so that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
    	DbFile df = Database.getCatalog().getDatabaseFile(tableId);
    	List<Page> lp = df.insertTuple(tid, t);
    	for(int i=0;i<lp.size();i++){
    		lp.get(i).markDirty(true, tid);
    		pageStore.put(lp.get(i).getId().hashCode(), lp.get(i));
    	}
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from. May block if
     * the lock cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit.  Does not need to update cached versions of any pages that have 
     * been dirtied, as it is not possible that a new page was created during the deletion
     * (note difference from addTuple).
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
    	DbFile df = Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId());
    	Page p = df.deleteTuple(tid, t);
    	p.markDirty(true, tid);
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1
    	Iterator<Integer> it = pageStore.keySet().iterator();
    	while(it.hasNext()){
    		int pid = it.next();
    		Page page = pageStore.get(pid);
    		flushPage(page.getId());
    	}
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
    */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // only necessary for lab5
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1
    	if(pageStore.containsKey(pid.hashCode())){
    		Page page = pageStore.get(pid.hashCode());
    		TransactionId tid = page.isDirty();
    		if(tid != null){
    			DbFile dbfile = Database.getCatalog().getDatabaseFile(pid.getTableId());
    			dbfile.writePage(page);
    			page.markDirty(false, tid);
    			pageStore.put(pid.hashCode(),page);
    		}
    	}
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    	Set<PageId> pids = lockManager.getDirtyPageIds(tid);
    	for(PageId pid: pids){
    		flushPage(pid);
    	}
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1
    	Iterator<Integer> it = pageStore.keySet().iterator();
    	while(it.hasNext()){
    		int pid = it.next();
    		Page page = pageStore.get(pid);
    		if(page.isDirty() == null){
    			try {
    				flushPage(page.getId());
    				pageStore.remove(pid);
    				
    				// unlock all the locks this page hold
    				lockManager.unlock(page.getId());
    			} catch (IOException e) {
    				// TODO Auto-generated catch block
    				e.printStackTrace();
    				throw new DbException("evictPage failed " + e.toString());
    			}
    			
    			return;
    		}
    	}
    	
    	throw new DbException("evict page failed without a clean page!");
    }

}
