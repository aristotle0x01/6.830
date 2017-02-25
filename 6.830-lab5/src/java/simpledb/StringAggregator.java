package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {
	private final int gbfield;
	private final Type gbfieldtype;
	private final Map<String,Integer> aggregator;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
    	if(!what.COUNT.equals(what)){
    		throw new IllegalArgumentException("StringAggregator only support COUNT");
    	}
    	
    	this.gbfield = gbfield;
    	this.gbfieldtype = gbfieldtype;
    	aggregator = new HashMap<String,Integer>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
    	if(gbfieldtype == null){ // no grouping
    		if(aggregator.containsKey("NO_GROUPING")){
    			
    			int nvalue = aggregator.get("NO_GROUPING") + 1;
    			aggregator.put("NO_GROUPING", nvalue);
    		}else{
    			aggregator.put("NO_GROUPING", 1);
    		}
    	}else{
    		String gbValue = new String("");
    		if(Type.INT_TYPE.equals(gbfieldtype)){
    			IntField field = (IntField)tup.getField(gbfield);
    			gbValue = field.getValue() + "";
    		}else{
    			StringField field = (StringField)tup.getField(gbfield);
    			gbValue = field.getValue();
    		}
    		
    		if(aggregator.containsKey(gbValue)){
    			int nvalue = aggregator.get(gbValue)+1;
    			aggregator.put(gbValue, nvalue);
    		}else{
    			aggregator.put(gbValue, 1);
    		}
    	}
    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public DbIterator iterator() {
        // some code goes here
    	List<Tuple> list = new ArrayList<Tuple>();
    	if(gbfieldtype == null){ // no grouping
    		Type[] type = new Type[1];
    		type[0] = Type.INT_TYPE;
    		TupleDesc desc = new TupleDesc(type);
    		Tuple tup = new Tuple(desc);
    		
    		int value = aggregator.get("NO_GROUPING");
    		IntField intfield = new IntField(value);
    		tup.setField(0, intfield);
    		list.add(tup);
    		return new TupleIterator(desc,list);
    	}else{
    		Type[] type = new Type[2];
    		type[0] = gbfieldtype;
    		type[1] = Type.INT_TYPE;
    		TupleDesc desc = new TupleDesc(type);
    		
    		for(String k:aggregator.keySet()){
    			Tuple tup = new Tuple(desc);
    			
    			Field gbValue = null;
        		if(Type.INT_TYPE.equals(gbfieldtype)){
        			gbValue = new IntField(Integer.parseInt(k));
        		}else{
        			gbValue = new StringField(k, Type.STRING_TYPE.getLen());
        		}
    			
        		int value = aggregator.get(k);
        		IntField intfield = new IntField(value);
        		tup.setField(0, gbValue);
        		tup.setField(1, intfield);
        		
        		list.add(tup);
    		}
    		
    		return new TupleIterator(desc,list);
    	}
    }

}
