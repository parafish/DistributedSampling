package pre.mapper.pair;

import java.io.IOException;
import java.math.BigInteger;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import pre.mapper.PreMapper;
import setting.PARAMETERS;

public abstract class AbstractPairMapper extends PreMapper
{
	protected abstract <T> BigInteger calcWeight(T[] items1, T[] items2);


	@Override
	protected void map(Object key, Text value, Context context) throws IOException,
					InterruptedException
	{
		String[] records = value.toString().split(PARAMETERS.SepRecords);

		BigInteger weight = calcWeight(records[0].split(PARAMETERS.SepItems),
						records[1].split(PARAMETERS.SepItems));

		String output = key.toString() + PARAMETERS.SepIndexWeight + weight.toString();

		context.write(NullWritable.get(), new Text(output));
	}
}
