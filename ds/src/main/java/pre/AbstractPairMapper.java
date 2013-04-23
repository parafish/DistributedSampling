package pre;

import java.io.IOException;
import java.math.BigInteger;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import util.Config;

/**
 * Maps indices/records to indices/weights. <p>
 * This class is designed for 
 * the discriminativity and squared frequency sampling, which requires the input 
 * as a pair of records. The method <code>map</code> receives a pair of records, and outputs
 * the weights calculated by the method<code>calcWeight</code>.
 * 
 * The input must be <code>CartesianInputFormat</code>.
 * @see DiscriminativityMapper
 * @see SquaredFreqMapper
 * @see CartesianProduct
 * @author zheyi
 *
 */
public abstract class AbstractPairMapper extends AbstractPreMapper
{
	/**
	 * Calculates the weight of a pair of records
	 * @param items1 the first record (a set of items)
	 * @param items2 the second record (a set of items)
	 * @return the weight of the pair of records
	 */
	protected abstract <T> BigInteger calcWeight(T[] items1, T[] items2);
	
	public void map(Writable key, Text value, OutputCollector<Writable, LongWritable> output, Reporter reporter) throws IOException
	{
		String[] records = value.toString().split(Config.SepRecords);

		BigInteger weight = calcWeight(records[0].split(Config.SepItems),
						records[1].split(Config.SepItems));

		output.collect(key, new LongWritable(0));
	}
}
