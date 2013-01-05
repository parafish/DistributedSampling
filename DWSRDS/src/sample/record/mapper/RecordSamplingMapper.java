package sample.record.mapper;

import java.io.IOException;
import java.math.BigInteger;
import java.util.Random;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import setting.NAMES;

public class RecordSamplingMapper extends Mapper<Text, Text, Text, IntWritable>
{
	private static final IntWritable one = new IntWritable(1);
	private BigInteger totalweight = null;
	private String nSamplers = null;

	@Override
	public void setup(Context context)
	{
		// XXX: change a better way to pass arguments
		totalweight = new BigInteger(context.getConfiguration().get(NAMES.TOTALWEIGHT.toString()));
		nSamplers = context.getConfiguration().get(NAMES.NSAMPLES.toString());
	}


	@Override
	public void map(Text key, Text value, Context context) throws IOException, InterruptedException
	{
		Random random = new Random();
		BigInteger randBigInteger = new BigInteger(totalweight.bitLength(), random);
		while (randBigInteger.compareTo(totalweight) >= 0)
			randBigInteger = new BigInteger(totalweight.bitLength(), random);
		
		BigInteger weight = new BigInteger(value.toString());
		if (weight.multiply(new BigInteger(nSamplers)).compareTo(randBigInteger) > 0)
			context.write(key, one);
	}
}
