package powermonitoring.udf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.pig.EvalFunc;
import org.apache.pig.FuncSpec;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

public class Median extends EvalFunc<Integer> {

	@Override
	public Integer exec(Tuple input) throws IOException {
		int result;
		
		if(input==null||input.size()==0){
			result=-1;
		}
		
		try{
			int control=input.size()%2;
			
			if(control==1){
				result= (Integer) input.get(input.size()/2);
			}else{
				int data1=(Integer) input.get(input.size()/2);
				int data2=(Integer) input.get(input.size()/2 + 1);
				result= (data1+data2)/2;
			}
		}catch(ExecException e){
			throw new IOException(e);
		}
		return result;
	}
	
	@Override
	public List<FuncSpec> getArgToFuncMapping() throws FrontendException{
		List<FuncSpec> funcSpecs=new ArrayList<FuncSpec>();
		funcSpecs.add(new FuncSpec(this.getClass().getName(), new Schema(new Schema.FieldSchema(null, DataType.INTEGER))));
		return funcSpecs;
	}

	
}
