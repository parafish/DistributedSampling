package test.sample.record;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.apfloat.Apfloat;
import org.apfloat.Apint;
import org.junit.Ignore;
import org.junit.Test;

import sample.record.mapper.RecordSamplingMapper;
import sample.record.mapper.RecordSamplingMapper.ReserviorOneSampler;
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

		new MapDriver<NullWritable, Text, NullWritable, Text>().withConfiguration(conf)
						.withMapper(new RecordSamplingMapper())
						.withInput(NullWritable.get(), new Text("5555&&&index 5556"))
						.withInput(NullWritable.get(), new Text("9999&&&index 5555")).runTest();
	}


	@Ignore
	@Test
	public void testRecordSamplingReducer()
	{
		int n = 100;
		List<Text> inputvalues = new ArrayList<Text>();
		for (int i = 0; i < 2 * n; i++)
			inputvalues.add(new Text("index" + i + " " + i));

		Configuration conf = new Configuration();
		conf.set(PARAMETERS.N_SAMPLES, String.valueOf(n));

		new ReduceDriver<NullWritable, Text, NullWritable, Text>().withConfiguration(conf)
						.withReducer(new RecordSamplingReducer())
						.withInput(NullWritable.get(), inputvalues).runTest();

	}


	@Test
	public static void testReplacement()
	{
		Random random = new Random();
		int nPopulation = 10;
		int nSample = 10;
		List<Pair<Apfloat, String>> population = new ArrayList<Pair<Apfloat, String>>();

		List<ReserviorOneSampler> instances = new ArrayList<ReserviorOneSampler>(
						nSample);
		for (int i = 0; i < nSample; i++)
			instances.add(new ReserviorOneSampler());

		for (int i = 7; i <= nPopulation; i++)
		{
			population.add(new Pair<Apfloat, String>(new Apint(i), String.valueOf(i)));
		}

		for (Pair<Apfloat, String> pair : population)
		{
			for (ReserviorOneSampler sampler : instances)
				sampler.sample(pair.getFirst().toString(), pair.getSecond());
		}

		for (ReserviorOneSampler sampler : instances)
		{
			System.out.println(sampler.getValue().toString() + ":\t" + sampler.getKey());
		}
	}


	// @Test
	// public void testReservoirSamper()
	// {
	// Random random = new Random();
	// int nPopulation = 200;
	// int nSample = 10;
	// List<Pair<Apint, String>> population = new ArrayList<Pair<Apint,
	// String>>();
	// ReserviorOneSampler sampler = new ReserviorOneSampler(nSample);
	//
	// for (int i = 1; i <= nPopulation; i++)
	// {
	// population.add(new Pair<Apint, String>(new Apint(new
	// BigInteger("2").pow(random.nextInt(i))),
	// String.valueOf(i)));
	// }
	// int i = 0;
	// for (Pair<Apint, String> pair : population)
	// {
	// sampler.sample(pair.getFirst().toString(true), pair.getSecond());
	// i++;
	// }
	//
	// System.out.println("Done. size: " + sampler.getReservior().size());
	// for (Pair<Apfloat, Object> pair : sampler.getReservior())
	// {
	// System.out.println(pair.getSecond().toString() + "(" +
	// pair.getFirst().precision()
	// + "):\t" + pair.getFirst().toString(true));
	// }
	// }

	public static void main(String[] args)
	{
		testReplacement();
	}
}
