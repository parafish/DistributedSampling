package test.sample.pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Ignore;
import org.junit.Test;

import sample.pattern.mapper.AreaFreqSamplingMapper;
import sample.pattern.mapper.DiscriminitivitySamplingMapper;
import sample.pattern.mapper.FreqSamplingMapper;
import sample.pattern.mapper.SquaredFreqSamplingMapper;
import setting.PARAMETERS;

public class PatternSamplingTest
{	
	@Ignore
	@Test
	public void testFreqPattern()
	{
		Configuration conf = new Configuration();
		conf.set(PARAMETERS.LEFT_PATH, "/home/zheyi/sampling/data/test.dat");
		
		new MapDriver<NullWritable, Text, NullWritable, Text>()
			.withConfiguration(conf)
			.withMapper(new FreqSamplingMapper())
			.withInput(NullWritable.get(), new Text("0"))
			.runTest();
	}
	
	@Test
	public void testAreaFreqPattern()
	{
		Configuration conf = new Configuration();
		conf.set(PARAMETERS.LEFT_PATH, "/home/zheyi/sampling/data/test.dat");
		
		new MapDriver<NullWritable, Text, NullWritable, Text>()
			.withConfiguration(conf)
			.withMapper(new AreaFreqSamplingMapper())
			.withInput(NullWritable.get(), new Text("0"))
			.runTest();
	}
	
	@Ignore
	@Test
	public void testDiscriminitivityPattern()
	{
		Configuration conf = new Configuration();
		conf.set(PARAMETERS.LEFT_PATH, "/home/zheyi/sampling/data/test.dat");
		conf.set(PARAMETERS.RIGHT_PATH, "/home/zheyi/sampling/data/test.dat");
		
		new MapDriver<NullWritable, Text, NullWritable, Text>()
			.withConfiguration(conf)
			.withMapper(new DiscriminitivitySamplingMapper())
			.withInput(NullWritable.get(), new Text("0&12"))
			.runTest();
	}
	
	@Ignore
	@Test
	public void testSquaredFreqPattern()
	{
		Configuration conf = new Configuration();
		conf.set(PARAMETERS.LEFT_PATH, "/home/zheyi/sampling/data/test.dat");
		
		new MapDriver<NullWritable, Text, NullWritable, Text>()
			.withConfiguration(conf)
			.withMapper(new SquaredFreqSamplingMapper())
			.withInput(NullWritable.get(), new Text("0&0"))
			.runTest();
	}
}
