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
import setting.NAMES;

public class PatternSamplingTest
{
	private Text OnlyKey = new Text("1");	
	
	@Ignore
	@Test
	public void testFreqPattern()
	{
		Configuration conf = new Configuration();
		conf.set(NAMES.ORI_FILE_1.name(), "/home/zheyi/sampling/data/test.dat");
		
		new MapDriver<Text, Text, Text, NullWritable>()
			.withConfiguration(conf)
			.withMapper(new FreqSamplingMapper())
			.withInput(OnlyKey, new Text("0"))
			.runTest();
	}
	
	@Test
	public void testAreaFreqPattern()
	{
		Configuration conf = new Configuration();
		conf.set(NAMES.ORI_FILE_1.name(), "/home/zheyi/sampling/data/test.dat");
		
		new MapDriver<Text, Text, Text, NullWritable>()
			.withConfiguration(conf)
			.withMapper(new AreaFreqSamplingMapper())
			.withInput(OnlyKey, new Text("0"))
			.runTest();
	}
	
	@Ignore
	@Test
	public void testDiscriminitivityPattern()
	{
		Configuration conf = new Configuration();
		conf.set(NAMES.ORI_FILE_1.name(), "/home/zheyi/sampling/data/test.dat");
		conf.set(NAMES.ORI_FILE_2.name(), "/home/zheyi/sampling/data/test.dat");
		
		new MapDriver<Text, Text, Text, NullWritable>()
			.withConfiguration(conf)
			.withMapper(new DiscriminitivitySamplingMapper())
			.withInput(OnlyKey, new Text("0&12"))
			.runTest();
	}
	
	@Ignore
	@Test
	public void testSquaredFreqPattern()
	{
		Configuration conf = new Configuration();
		conf.set(NAMES.ORI_FILE_1.name(), "/home/zheyi/sampling/data/test.dat");
		
		new MapDriver<Text, Text, Text, NullWritable>()
			.withConfiguration(conf)
			.withMapper(new SquaredFreqSamplingMapper())
			.withInput(OnlyKey, new Text("0&0"))
			.runTest();
	}
}
