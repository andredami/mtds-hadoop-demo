package powermonitoring.udf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.pig.EvalFunc;
import org.apache.pig.FuncSpec;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

public class Median extends EvalFunc<Double> {

	@Override
	public Double exec(Tuple input) throws IOException {
		DataBag values;
		Double result;
		
		if(input==null||input.size()==0){
			result=-1.0;
		}
		
		try{
			values = (DataBag)input.get(0);
			Iterator<Tuple> i = values.iterator();
			LinkedList<Integer> list = new LinkedList<Integer>();
			while(i.hasNext()){
				Tuple t = (Tuple) i.next();
				list.add((Integer)t.get(0));
			}
			Collections.sort(list);
			
			int control=list.size()%2;
			
			if(control==1){
				int tmp = list.get(list.size()/2);
				result = (double) tmp;
			}else{
				int data1=(Integer) list.get(list.size()/2);
				int data2=(Integer) list.get(list.size()/2 + 1);
				result= ((double)data1+(double)data2)/2.0;
			}
		}catch(ExecException e){
			throw new IOException(e);
		}
		return result;
	}
	
	@Override
	public List<FuncSpec> getArgToFuncMapping() throws FrontendException{
		List<FuncSpec> funcSpecs=new ArrayList<FuncSpec>();
		funcSpecs.add(new FuncSpec(this.getClass().getName(), new Schema(new Schema.FieldSchema(null, DataType.BAG))));
		return funcSpecs;
	}

	
}
