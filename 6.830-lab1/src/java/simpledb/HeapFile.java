package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {
	
	private final File file;
	private final TupleDesc td;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
    	this.file = f;
    	this.td = td;
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
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
    	int tableId = pid.getTableId();
    	int pgNo = pid.pageNumber();
    	
    	RandomAccessFile f=null;
    	try{
    		f = new RandomAccessFile(file, "r");
        	if((pgNo+1)*BufferPool.getPageSize() > f.length()){
        		f.close();
        		throw new IllegalArgumentException(String.format("table %d page %d is invalid", tableId, pgNo));
        	}
        	
    		byte[] bytes = new byte[BufferPool.getPageSize()];
        	f.seek(pgNo*BufferPool.getPageSize());
        	int read = f.read(bytes, 0, BufferPool.getPageSize());
        	if(read != BufferPool.getPageSize()){
        		throw new IllegalArgumentException(String.format("table %d page %d read %d bytes", tableId, pgNo, read));
        	}
        	
        	HeapPageId id = new HeapPageId(pid.getTableId(),pid.pageNumber());
        	return new HeapPage(id,bytes);
    	}catch(IOException e){
    		e.printStackTrace();
    	}finally{
    		try{
    			f.close();
    		}catch(Exception e){
    			e.printStackTrace();
    		}
    	}
    	
    	throw new IllegalArgumentException(String.format("table %d page %d is invalid", tableId, pgNo));
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
        // some code goes here
    	int num = (int)Math.floor(file.length() / BufferPool.getPageSize());
        return num;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public Page deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(this, tid);
    }

    private static final class HeapFileIterator implements DbFileIterator{
    	
    	private final HeapFile heapFile;
    	private final TransactionId tid;
    	private Iterator<Tuple> it;
    	private final ArrayList<Tuple> al;
    	
    	public HeapFileIterator(HeapFile file, TransactionId tid){
    		heapFile = file; 
    		this.tid = tid;
    		al = new ArrayList<Tuple>();
    		it = al.iterator();
    	}

		@Override
		public void open() throws DbException, TransactionAbortedException {
			// TODO Auto-generated method stub
			al.clear();
			
			for(int i=0;i<heapFile.numPages();i++){
				HeapPageId pid = new HeapPageId(heapFile.getId(),i);
				HeapPage page = (HeapPage)Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
				Iterator<Tuple> pit = page.iterator();
				while(pit.hasNext()){
					al.add(pit.next());
				}
			}
			
			it = al.iterator();
		}

		@Override
		public boolean hasNext() throws DbException, TransactionAbortedException {
			// TODO Auto-generated method stub
			return it.hasNext();
		}

		@Override
		public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
			// TODO Auto-generated method stub
			return it.next();
		}

		@Override
		public void rewind() throws DbException, TransactionAbortedException {
			// TODO Auto-generated method stub
			open();
		}

		@Override
		public void close() {
			// TODO Auto-generated method stub
			al.clear();
			it = al.iterator();
		}
    	
    }
}

