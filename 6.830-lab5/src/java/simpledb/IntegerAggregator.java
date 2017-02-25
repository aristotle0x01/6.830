package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {
	private final int gbfield;
	private final Type gbfieldtype;
	private final int afield;
	private final Op what;
	private final Map<String,Integer> aggregator;
	private final Map<String,Integer> counter;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
    	this.gbfield = gbfield;
    	this.gbfieldtype = gbfieldtype;
    	this.afield = afield;
    	this.what = what;
    	aggregator = new HashMap<String,Integer>();
    	counter = new HashMap<String,Integer>();
    }
    
    private int calc(int oldV, int newV){
    	if(what.MIN.equals(what)){
    		return oldV > newV ? newV : oldV;
    	}else if(what.MAX.equals(what)){
    		return oldV > newV ? oldV : newV;
    	}else if(what.SUM.equals(what)){
    		return oldV + newV;
    	}else if(what.AVG.equals(what)){
    		return oldV + newV;
    	}else{
    		return oldV + 1;
    	}
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
    	if(gbfieldtype == null){ // no grouping
    		IntField field = (IntField)tup.getField(afield);
    		if(aggregator.containsKey("NO_GROUPING")){
    			
    			int nvalue = calc(aggregator.get("NO_GROUPING"),field.getValue());
    			aggregator.put("NO_GROUPING", nvalue);
    			counter.put("NO_GROUPING", 1+counter.get("NO_GROUPING"));
    		}else{
    			aggregator.put("NO_GROUPING", field.getValue());
    			counter.put("NO_GROUPING", 1);
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
    		
    		
    		IntField field = (IntField)tup.getField(afield);
    		if(aggregator.containsKey(gbValue)){
    			int nvalue = calc(aggregator.get(gbValue),field.getValue());
    			aggregator.put(gbValue, nvalue);
    			counter.put(gbValue, 1+counter.get(gbValue));
    		}else{
    			aggregator.put(gbValue, what.COUNT.equals(what) ? 1 : field.getValue());
    			counter.put(gbValue, 1);
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
    		
    		if(!aggregator.isEmpty()){
    			int value = aggregator.get("NO_GROUPING");
        		if(what.AVG.equals(what)){
        			value = value/counter.get("NO_GROUPING");
        		}
        		IntField intfield = new IntField(value);
        		tup.setField(0, intfield);
        		list.add(tup);
    		}
    		
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
        		if(what.AVG.equals(what)){
        			value = value/counter.get(k);
        		}
        		
        		IntField intfield = new IntField(value);
        		tup.setField(0, gbValue);
        		tup.setField(1, intfield);
        		
        		list.add(tup);
    		}
    		
    		return new TupleIterator(desc,list);
    	}
    }

}
