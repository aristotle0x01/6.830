package simpledb;

import java.io.IOException;

/**
 * The delete operator.  Delete reads tuples from its child operator and
 * removes them from the table they belong to.
 */
public class Delete extends Operator {
	private final DbIterator child;
	private final TransactionId t;
	private boolean called = false;
	
    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * @param t The transaction this delete runs in
     * @param child The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, DbIterator child) {
        // some code goes here
    	this.t = t;
    	this.child = child;
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
    	child.close();
    	child.open();
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
    	if(called){
    		return null;
    	}
    	called = true;
    	
    	int count = 0;
    	
    	while(child.hasNext()){
    		Tuple next = child.next();
    		Database.getBufferPool().deleteTuple(t, next);
			count++;
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
