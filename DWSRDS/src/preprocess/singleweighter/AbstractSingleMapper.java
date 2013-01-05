package preprocess.singleweighter;

import java.io.IOException;
import java.math.BigInteger;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

/**
 * 
 * @author zheyi
 *
 */
public abstract class AbstractSingleMapper extends Mapper<LongWritable, Text, Text, Text>
{
	protected abstract <T> BigInteger calcWeight(T [] items);
	
	@Override
	public void map(LongWritable key, Text value, Context context)
	throws IOException, InterruptedException
	{
		String [] items = value.toString().split(" ");		// TODO: change " " (separator) to a variable
		BigInteger weight = calcWeight(items);
		
		context.write(new Text(key.toString()) ,  new Text(weight.toString()));
	}
}
