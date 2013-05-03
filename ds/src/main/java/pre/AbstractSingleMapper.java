package pre;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import util.Config;
import freq.FreqMapper;


/**
 * Maps each offset/text pair to offset/weight pairs.
 * <p>
 * Subclasses should implement the weight calculation function.
 * 
 * @see FreqMapper
 * @see AreaFreqMapper
 * @author zheyi
 */
public abstract class AbstractSingleMapper extends AbstractPreMapper
{
	/**
	 * Calculates the weight of an array.
	 * 
	 * @param items
	 *            the array of items
	 * @return the weight of the array of items
	 */
	protected abstract <T> long calcWeight(T[] items);


	@Override
	public void map(Writable key, Text value, OutputCollector<Writable, LongWritable> output, Reporter reporter)
					throws IOException
	{
		String[] items = value.toString().trim().split(Config.SepItemsRegex);
		String outputkey = filepath + Config.SepFilePosition + key.toString();
//			System.out.println("outputkey: " + outputkey);
		output.collect(new Text(outputkey), new LongWritable(calcWeight(items)));
	}
	
}
