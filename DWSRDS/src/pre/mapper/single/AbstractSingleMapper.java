package pre.mapper.single;

import java.io.IOException;
import java.math.BigInteger;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import pre.mapper.PreMapper;
import setting.PARAMETERS;

/**
 * 
 * @author zheyi
 * 
 */
public abstract class AbstractSingleMapper extends PreMapper
{
	protected abstract <T> BigInteger calcWeight(T[] items);

	@Override
	public void map(Object key, Text value, Context context) throws IOException,
					InterruptedException
	{
		String[] items = value.toString().split(PARAMETERS.SepItems);
		BigInteger weight = calcWeight(items);

		String output = key.toString() + PARAMETERS.SepIndexWeight + weight.toString();
		context.write(NullWritable.get(), new Text(output));
	}
}
