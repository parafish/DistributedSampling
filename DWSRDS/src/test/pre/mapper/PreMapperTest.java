package test.pre.mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Test;

import pre.mapper.pair.DiscriminitivityMapper;
import pre.mapper.pair.SquaredFreqMapper;
import pre.mapper.single.AreaFreqMapper;
import pre.mapper.single.FreqMapper;

public class PreMapperTest
{
	private Text record = new Text("2 3 4 5 6");
	private Text pairRecords = new Text("1 2 3 4,2 3 4 5");
	
	@Test
	public void testFreqWeightMapper()
	{
		new MapDriver<Object, Text, NullWritable, Text>()
			.withMapper(new FreqMapper())
			.withInput(new LongWritable(100000000000000L), record)
			.withOutput(NullWritable.get(), new Text("100000000000000 32"))
			.runTest();
	}
	
	@Test
	public void testAreaFreqWeightMapper()
	{
		new MapDriver<Object, Text, NullWritable, Text>()
			.withMapper(new AreaFreqMapper())
			.withInput(new LongWritable(0), record)
			.withOutput(NullWritable.get(), new Text("0 80"))
			.runTest();
	}
	
	@Test
	public void testDiscriminitivityWeightMapper()
	{
		new MapDriver<Object, Text, NullWritable, Text>()
			.withMapper(new DiscriminitivityMapper())
			.withInput("0,0", pairRecords)
			.withOutput(NullWritable.get(), new Text("0,0 8"))
			.runTest();
	}

	@Test
	public void testSquaredFreqWeightMapper()
	{		
		new MapDriver<Object, Text, NullWritable, Text>()
			.withMapper(new SquaredFreqMapper())
			.withInput("0,0", pairRecords)
			.withOutput(NullWritable.get(), new Text("0,0 8"))
			.runTest();
	}

}
