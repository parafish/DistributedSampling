package util;

import static org.junit.Assert.assertTrue;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.thirdparty.guava.common.collect.ConcurrentHashMultiset;
import org.apache.hadoop.thirdparty.guava.common.collect.Multiset;
import org.junit.Ignore;
import org.junit.Test;


public class ReservoirOneSamplerTest
{
	Random random = new Random();

	@Ignore
	@Test
	public void testWritable()
	{
		int k = 1000; // number of samples
		Text item1 = new Text("item1");
		Text item2 = new Text("item2");

		Map<Writable, String> population = new TreeMap<Writable, String>();
		population.put(item1, "8");
		population.put(item2, "4");
		// population.put("item3", "2000");

		List<ReservoirOneSampler> samplers = new ArrayList<ReservoirOneSampler>(k);

		for (int i = 0; i < k; i++)
			samplers.add(new ReservoirOneSampler());

		for (Map.Entry<Writable, String> item : population.entrySet())
			for (ReservoirOneSampler sampler : samplers)
				sampler.sample(item.getValue(), item.getKey());

		Multiset<String> stats = ConcurrentHashMultiset.create();

		for (ReservoirOneSampler sampler : samplers)
		{
			stats.add(sampler.getItem().toString());
		}
		System.out.println("Population: " + k);
		for (String item : stats.elementSet())
			System.out.println(item + "\t" + stats.count(item));

		float error = stats.count("item1") * 1.0f / stats.count("item2") - 2.0f;

		assertTrue("see the proportion", Math.abs(error) < 0.2f);
	}
	
	@Ignore
	@Test
	public void testOneBigWeight()
	{
		int k = 10000;
		int n = 20;
		int exp = 300;
		
		Map<String, String> population = new TreeMap<String, String>();
		population.put("item1", new BigInteger("2").pow(20).toString());
		population.put("item2", new BigInteger("2").pow(19).toString());
		population.put("item3", new BigInteger("2").pow(exp).toString());
		for(int i =3; i<n; i++)
		{
			population.put("other" + String.valueOf(i), new BigInteger("2").pow(random.nextInt(15)).toString());
		}

		List<ReservoirOneSampler> samplers = new ArrayList<ReservoirOneSampler>(k);

		for (int i = 0; i < k; i++)
			samplers.add(new ReservoirOneSampler());

		for (Map.Entry<String, String> item : population.entrySet())
			for (ReservoirOneSampler sampler : samplers)
				sampler.sample(item.getValue(), item.getKey());
		
		Multiset<String> stats = ConcurrentHashMultiset.create();

		for (ReservoirOneSampler sampler : samplers)
			stats.add((String) sampler.getItem());

		System.out.println("Population: " + k + "\tMax exp: " + exp);
		System.out.println("Precision: " + ReservoirOneSampler.getPrecision());
		for (String item : stats.elementSet())
			System.out.println(item + "\t" + stats.count(item));
		
		float error = stats.count("item1") * 1.0f / stats.count("item2") - 2.0f;

		assertTrue("see the proportion", Math.abs(error) < 0.2f);
	}
	
	/**
	 * when precision is enough, correct.
	 * Otherwise, not correct
	 */
	@Test
	public void testBigWeight()			
	{
		int k = 2000;
		int exp = 300;
		
		Map<String, String> population = new TreeMap<String, String>();
		population.put("item1", new BigInteger("2").pow(exp).toString());
		population.put("item2", new BigInteger("2").pow(exp-1).toString());
		population.put("item3", "1024");

		List<ReservoirOneSampler> samplers = new ArrayList<ReservoirOneSampler>(k);

		for (int i = 0; i < k; i++)
			samplers.add(new ReservoirOneSampler());

		for (Map.Entry<String, String> item : population.entrySet())
			for (ReservoirOneSampler sampler : samplers)
				sampler.sample(item.getValue(), item.getKey());
		
		Multiset<String> stats = ConcurrentHashMultiset.create();

		for (ReservoirOneSampler sampler : samplers)
			stats.add((String) sampler.getItem());

		System.out.println("Population: " + k + "\tMax exp: " + exp);
		System.out.println("Precision: " + ReservoirOneSampler.getPrecision());
		for (String item : stats.elementSet())
			System.out.println(item + "\t" + stats.count(item));
		
		float error = stats.count("item1") * 1.0f / stats.count("item2") - 2.0f;

		assertTrue("see the proportion", Math.abs(error) < 0.2f);
	}


	// test correctness of the sampling class
	@Test
	public void testCorrectness()
	{
		int k = 1000; // number of samples

		Map<String, String> population = new TreeMap<String, String>();
		population.put("item1", "8");
		population.put("item2", "4");
		// population.put("item3", "2000");

		List<ReservoirOneSampler> samplers = new ArrayList<ReservoirOneSampler>(k);

		for (int i = 0; i < k; i++)
			samplers.add(new ReservoirOneSampler());

		for (Map.Entry<String, String> item : population.entrySet())
			for (ReservoirOneSampler sampler : samplers)
				sampler.sample(item.getValue(), item.getKey());

		Multiset<String> stats = ConcurrentHashMultiset.create();

		for (ReservoirOneSampler sampler : samplers)
		{
			stats.add((String) sampler.getItem());
		}
		System.out.println("Population: " + k);
		for (String item : stats.elementSet())
			System.out.println(item + "\t" + stats.count(item));

		float error = stats.count("item1") * 1.0f / stats.count("item2") - 2.0f;

		assertTrue("see the proportion", Math.abs(error) < 0.2f);
	}


	/**
	 * Speed test
	 * 
	 * @param args
	 */
	public static void main(String args[])
	{
		Random random = new Random();
		int k = 1000; // number of samples
		int n = 100; // number of population

		Map<String, String> population = new TreeMap<String, String>();
		for (int i = 0; i < n; i++)
			population.put("item" + String.valueOf(i), String.valueOf(random.nextInt(n)));

		List<ReservoirOneSampler> samplers_low = new ArrayList<ReservoirOneSampler>(k);
		List<ReservoirOneSampler> samplers_med = new ArrayList<ReservoirOneSampler>(k);
		List<ReservoirOneSampler> samplers_high = new ArrayList<ReservoirOneSampler>(k);

		for (int i = 0; i < k; i++)
		{
			samplers_low.add(new ReservoirOneSampler());
			samplers_med.add(new ReservoirOneSampler(60));
			samplers_high.add(new ReservoirOneSampler(ReservoirOneSampler.maximumPrecision));
		}
		{
			long start = System.currentTimeMillis();
			for (Map.Entry<String, String> item : population.entrySet())
				for (ReservoirOneSampler sampler : samplers_low)
					sampler.sample(item.getValue(), item.getKey());
			long end = System.currentTimeMillis();
			System.out.println(population.size() + " population, " + k + " samples, 20 precision\t" + (end - start));
		}

		{
			long start = System.currentTimeMillis();
			for (Map.Entry<String, String> item : population.entrySet())
				for (ReservoirOneSampler sampler : samplers_med)
					sampler.sample(item.getValue(), item.getKey());
			long end = System.currentTimeMillis();
			System.out.println(population.size() + " population, " + k + " samples, 60 precision\t" + (end - start));
		}

		{
			long start = System.currentTimeMillis();
			for (Map.Entry<String, String> item : population.entrySet())
				for (ReservoirOneSampler sampler : samplers_high)
					sampler.sample(item.getValue(), item.getKey());
			long end = System.currentTimeMillis();
			System.out.println(population.size() + " population, " + k + " samples, 100 precision\t" + (end - start));
		}
	}
}
