package simpledb;
import java.io.IOException;

/**
 * Inserts tuples read from the child operator into
 * the tableid specified in the constructor
 */
public class Insert extends Operator {
	private final TransactionId t;
	private final DbIterator child;
	private final int tableid;
	private boolean called = false;

    /**
     * Constructor.
     * @param t The transaction running the insert.
     * @param child The child operator from which to read tuples to be inserted.
     * @param tableid The table in which to insert tuples.
     * @throws DbException if TupleDesc of child differs from table into which we are to insert.
     */
    public Insert(TransactionId t, DbIterator child, int tableid)
        throws DbException {
        // some code goes here
    	this.t = t;
    	this.child = child;
    	this.tableid = tableid;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
    	Type[] type = new Type[1];
		type[0] = Type.INT_TYPE;
		TupleDesc desc = new TupleDesc(type);
        return desc;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
    	child.open();
    }

    public void close() {
        // some code goes here
    	child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
    	close();
    	open();
    }

    /**
     * Inserts tuples read from child into the tableid specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool.
     * An instances of BufferPool is available via Database.getBufferPool().
     * Note that insert DOES NOT need check to see if a particular tuple is
     * a duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
    * null if called more than once.
     * @throws  
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext()
            throws TransactionAbortedException, DbException {
        // some code goes here
    	if(called){
    		return null;
    	}
    	called = true;
    	
    	int count = 0;
    	
    	while(child.hasNext()){
    		try {
				Database.getBufferPool().insertTuple(t, tableid, child.next());
				count++;
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
    	}
    	
    	Type[] type = new Type[1];
		type[0] = Type.INT_TYPE;
		TupleDesc desc = new TupleDesc(type);
		Tuple tup = new Tuple(desc);
		IntField intfield = new IntField(count);
		tup.setField(0, intfield);
		
		return tup;
    }
}
