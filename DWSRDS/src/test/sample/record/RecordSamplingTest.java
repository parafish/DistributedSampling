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
import setting.PARAMETERS;

public class RecordSamplingTest
{	
	@Ignore
	@Test
	public void testRecordSamplingMapper()
	{
		Configuration conf = new Configuration();
		conf.set(PARAMETERS.N_SAMPLES, "10");
		
		new MapDriver<NullWritable, Text, NullWritable, Text>()
		.withConfiguration(conf)
		.withMapper(new RecordSamplingMapper())
		.withInput(NullWritable.get(), new Text("5555&&&index 5556"))
		.withInput(NullWritable.get(), new Text("9999&&&index 5555"))
		.runTest();
	}
	
	@Test
	public void testRecordSamplingReducer()
	{
		int n = 100;
		List<Text> inputvalues = new ArrayList<Text>();
		for (int i=0; i< 2*n ; i++)
			inputvalues.add(new Text("index" + i + " " + i));
			
		Configuration conf = new Configuration();
		conf.set(PARAMETERS.N_SAMPLES, String.valueOf(n));
		
		new ReduceDriver<NullWritable, Text, NullWritable, Text>()
			.withConfiguration(conf)
			.withReducer(new RecordSamplingReducer())
			.withInput(NullWritable.get(), inputvalues)
			.runTest();
			
	}
	
}
