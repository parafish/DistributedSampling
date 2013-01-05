package sample.record;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigInteger;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import setting.NAMES;

public class RecordSamplingMapper extends Mapper<Text, Text, Text, Text>
{
	private static final Text one = new Text("1");
	private BigInteger totalweight = null;
	private String nSamplers = null;

	@Override
	public void setup(Context context) throws IOException
	{
		// do some file splitting here
		Configuration conf = context.getConfiguration();
		FileSystem fs = FileSystem.get(conf);
		FileStatus[] status = fs.listStatus(new Path(conf.get(NAMES.TOTALWEIGHT_PATH.toString())));
		BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(status[0].getPath())));
		String totalweightString = reader.readLine().split("\t")[0];
		reader.close();
		// XXX: change a better way to pass arguments
		totalweight = new BigInteger(totalweightString);
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
