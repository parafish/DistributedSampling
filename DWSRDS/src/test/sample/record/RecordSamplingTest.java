package test.sample.record;

import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Ignore;
import org.junit.Test;

import sample.record.mapper.RecordSamplingMapper;
import sample.record.mapper.RecordSamplingMapper.ReserviorSampler;
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
	
	@Ignore
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
	
	@Test
	public void testReplacement()
	{
		int nPopulation = 100;
		int nSample = 10;
		List<Pair<Double, String>> population = new ArrayList<Pair<Double,String>>();
		
		List<ReserviorSampler> instances = new ArrayList<RecordSamplingMapper.ReserviorSampler>(nSample);
		for (int i =0; i< nSample ; i++)
			instances.add(new ReserviorSampler(1));
		
		for (int i=1; i<=nPopulation; i++)
		{
			population.add(new Pair<Double, String>((double)i, String.valueOf(i)));
		}
		
		for (Pair<Double, String> pair : population)
		{
			for (ReserviorSampler sampler : instances)
				sampler.sample(pair.getFirst().toString(), pair.getSecond());
		}
		
		for (ReserviorSampler sampler : instances)
		{
			PriorityQueue<Pair<Double,Object>> queue = sampler.getReservior();
			System.out.println(queue.peek().getSecond().toString() + ":\t" + queue.peek().getFirst());
		}
		
	}
	
	@Test 
	public void testReservoirSamper()
	{
		int nPopulation = 1000;
		int nSample = 10;
		List<Pair<Double, String>> population = new ArrayList<Pair<Double,String>>();
		ReserviorSampler sampler = new ReserviorSampler(nSample);
		
		for (int i=1; i<=nPopulation; i++)
		{
			population.add(new Pair<Double, String>((double)i, String.valueOf(i)));
		}
		
		for (Pair<Double, String> pair : population)
			sampler.sample(pair.getFirst().toString(), pair.getSecond());
		
		for (Pair<Double, Object> pair : sampler.getReservior())
		{
			System.out.println(pair.getSecond().toString() + ":\t" + pair.getFirst());
		}
	}
	
	public static void main(String [] args)
	{
		new RecordSamplingTest().testReplacement();
	}
}
