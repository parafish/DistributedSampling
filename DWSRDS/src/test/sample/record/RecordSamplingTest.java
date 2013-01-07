package test.sample.record;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Ignore;
import org.junit.Test;

import sample.record.mapper.RecordSamplingMapper;
import sample.record.reducer.RecordSamplingReducer;
import setting.NAMES;

public class RecordSamplingTest
{
	private Text OnlyKey = new Text("1");	
	
	@Test
	public void testRecordSamplingMapper()
	{
		Configuration conf = new Configuration();
		conf.set(NAMES.NSAMPLES.name(), "1000");
		
		new MapDriver<Text, Text, Text, Text>()
		.withConfiguration(conf)
		.withMapper(new RecordSamplingMapper())
		.withInput(OnlyKey, new Text("1111&&&index 1111"))
		.withInput(OnlyKey, new Text("9999&&&index 9999"))
		.runTest();
	}
	
	@Ignore
	@Test
	public void testRecordSamplingReducer()
	{
		List<Text> inputvalues = new ArrayList<Text>();
		inputvalues.add(new Text("index1111 0.9"));
		inputvalues.add(new Text("index2222 0.1"));
		
		Configuration conf = new Configuration();
		conf.set(NAMES.NSAMPLES.name(), "100");
		
		new ReduceDriver<Text, Text, Text, Text>()
			.withConfiguration(conf)
			.withReducer(new RecordSamplingReducer())
			.withInput(OnlyKey, inputvalues)
			.runTest();
			
	}
	
}
