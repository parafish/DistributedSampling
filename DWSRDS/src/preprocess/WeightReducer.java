package preprocess;

import java.io.IOException;
import java.math.BigInteger;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import setting.NAMES;

public class WeightReducer extends Reducer<Text, Text, Text, Text>
{
	private MultipleOutputs<Text, Text> multipleOutputs;
	private BigInteger totalweight = BigInteger.ZERO;
	private static String recordBasePath = NAMES.RECORD + "/part";
	private static String totalweightBasePath = NAMES.TOTALWEIGHT + "/part";
	
	@Override
	protected void setup(Context context)
	{
		multipleOutputs = new MultipleOutputs<Text, Text>(context);
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException
	{
		multipleOutputs.write(new Text(totalweight.toString()), new Text(), totalweightBasePath);
		multipleOutputs.close();
	}
	
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException
	{
		BigInteger localweight = BigInteger.ZERO;
		for (Text value : values)
		{
			localweight = localweight.add(new BigInteger(value.toString()));
		}
		multipleOutputs.write(key, new Text(localweight.toString()), recordBasePath);
		totalweight = totalweight.add(localweight);
	}
}
