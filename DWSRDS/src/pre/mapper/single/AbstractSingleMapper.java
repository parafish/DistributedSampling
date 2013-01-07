package pre.mapper.single;

import java.io.IOException;
import java.math.BigInteger;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import pre.mapper.PreMapper;

import setting.PARAMETERS;

/**
 * 
 * @author zheyi
 * 
 */
public abstract class AbstractSingleMapper extends PreMapper
{
	private Text OnlyKey = new Text("1"); // dummy key. to avoid sorting


	protected abstract <T> BigInteger calcWeight(T[] items);


	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException,
					InterruptedException
	{
		String[] items = value.toString().split(PARAMETERS.SeparatorItem);
		BigInteger weight = calcWeight(items);

		context.write(OnlyKey,
						new Text(key.toString() + PARAMETERS.SeparatorIndexWeight
										+ weight.toString()));
	}
}
