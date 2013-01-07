package test.pre.mapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Ignore;
import org.junit.Test;

import pre.mapper.pair.DiscriminitivityMapper;
import pre.mapper.pair.SquaredFreqMapper;
import pre.mapper.single.AreaFreqMapper;
import pre.mapper.single.FreqMapper;
import sample.pattern.mapper.FreqSamplingMapper;
import setting.NAMES;
import setting.PARAMETERS;

public class PreMapperTest
{
	private Text record = new Text("2 3 4 5 6");
	
	
	@Test
	public void testFreqWeightMapper()
	{
		new MapDriver<LongWritable, Text, Text, Text>()
			.withMapper(new FreqMapper())
			.withInput(new LongWritable(100000000000000L), record)
			.withOutput(new Text("1"), new Text("100000000000000 32"))
			.runTest();
	}
	
	@Ignore
	@Test
	public void testAreaFreqWeightMapper()
	{
		new MapDriver<LongWritable, Text, Text, Text>()
			.withMapper(new AreaFreqMapper())
			.withInput(new LongWritable(0), record)
			.withOutput(new Text("1"), new Text("0 80"))
			.runTest();
	}
	
	@Ignore
	@Test
	public void testDiscriminitivityWeightMapper()
	{
		Configuration conf = new Configuration();
		// must specify
		conf.set(NAMES.ORI_FILE_2.name(), "/home/zheyi/sampling/data/test.dat");
		
		new MapDriver<LongWritable, Text, Text, Text>()
			.withConfiguration(conf)
			.withMapper(new DiscriminitivityMapper())
			.withInput(new LongWritable(0), record)
			.withOutput(new Text("1"), new Text("0&0 30"))
			.withOutput(new Text("1"), new Text("0&4 28"))
			.withOutput(new Text("1"), new Text("0&8 28"))
			.withOutput(new Text("1"), new Text("0&12 28"))
			.runTest();
	}

	@Ignore
	@Test
	public void testSquaredFreqWeightMapper()
	{		
		new MapDriver<LongWritable, Text, Text, Text>()
			.withMapper(new SquaredFreqMapper())
			.withInput(new LongWritable(0), record)
			.withOutput(new Text("1"), new Text("0&0 2"))
			.withOutput(new Text("1"), new Text("0&4 4"))
			.withOutput(new Text("1"), new Text("0&8 4"))
			.withOutput(new Text("1"), new Text("0&12 4"))
			.runTest();
	}

}
