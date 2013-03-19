package pre;

import java.io.IOException;
import java.math.BigInteger;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import util.PARAMETERS;


/**
 * Maps each offset/text pair to offset/weight pairs. <p>
 * Subclasses should implement the weight calculation function.
 * @see FreqMapper
 * @see AreaFreqMapper
 * @author zheyi
 */
public abstract class AbstractSingleMapper extends AbstractPreMapper
{
	/**
	 * Calculates the weight of an array.
	 * @param 	items the array of items
	 * @return 	the weight of the array of items
	 */
	protected abstract <T> BigInteger calcWeight(T[] items);

	@Override
	public void map(Writable key, Text value, OutputCollector<Writable, Text> output,
					Reporter reporter) throws IOException
	{
		String[] items = value.toString().split(PARAMETERS.SepItems);		
		output.collect(key, new Text(calcWeight(items).toString()));
	}
}
