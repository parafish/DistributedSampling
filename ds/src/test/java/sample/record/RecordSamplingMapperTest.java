package sample.record;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

import org.apache.hadoop.thirdparty.guava.common.collect.ConcurrentHashMultiset;
import org.apache.hadoop.thirdparty.guava.common.collect.Multiset;
import org.junit.Test;

import sample.record.RecordSamplingMapper.ReserviorOneSampler;


public class RecordSamplingMapperTest
{

	@Test
	public void testRecordSamplingMapper()
	{
		// no way to test the mapper...

	}


	// test the sampling class
	@Test
	public void testReservoirOneSampler()
	{
		int n = 10000; // number of samples

		Map<String, String> population = new TreeMap<String, String>();
		population.put("item1", "8");
		population.put("item2", "4");
		// population.put("item3", "2000");

		List<ReserviorOneSampler> samplers = new ArrayList<ReserviorOneSampler>(n);

		for (int i = 0; i < n; i++)
			samplers.add(new ReserviorOneSampler(60));

		for (Map.Entry<String, String> item : population.entrySet())
			for (ReserviorOneSampler sampler : samplers)
				sampler.sample(item.getValue(), item.getKey());

		Multiset<String> stats = ConcurrentHashMultiset.create();

		for (ReserviorOneSampler sampler : samplers)
			stats.add((String) sampler.getItem());

		System.out.println("Population: " + n);
		for (String item : stats.elementSet())
			System.out.println(item + "\t" + stats.count(item));

		float error = stats.count("item1") * 1.0f / stats.count("item2") - 1.0f / 3;

		// assertTrue("see the proportion", Math.abs(error) < 0.05f);
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
		int n = 100;	// number of population
		
		Map<String, String> population = new TreeMap<String, String>();
		for (int i=0; i< n; i++)
			population.put("item" + String.valueOf(i), String.valueOf(random.nextInt(n)));
		
		List<ReserviorOneSampler> samplers_low = new ArrayList<ReserviorOneSampler>(k);
		List<ReserviorOneSampler> samplers_med = new ArrayList<ReserviorOneSampler>(k);
		List<ReserviorOneSampler> samplers_high = new ArrayList<ReserviorOneSampler>(k);

		for (int i = 0; i < k; i++)
		{
			samplers_low.add(new ReserviorOneSampler());
			samplers_med.add(new ReserviorOneSampler(60));
			samplers_high.add(new ReserviorOneSampler(ReserviorOneSampler.maximumPrecision));
		}
		{
			long start = System.currentTimeMillis();
			for (Map.Entry<String, String> item : population.entrySet())
				for (ReserviorOneSampler sampler : samplers_low)
					sampler.sample(item.getValue(), item.getKey());
			long end = System.currentTimeMillis();
			System.out.println(population.size() + " population, " + k + " samples, 20 precision\t" + (end - start));
		}

		{
			long start = System.currentTimeMillis();
			for (Map.Entry<String, String> item : population.entrySet())
				for (ReserviorOneSampler sampler : samplers_med)
					sampler.sample(item.getValue(), item.getKey());
			long end = System.currentTimeMillis();
			System.out.println(population.size() + " population, " + k + " samples, 60 precision\t" + (end - start));
		}

		{
			long start = System.currentTimeMillis();
			for (Map.Entry<String, String> item : population.entrySet())
				for (ReserviorOneSampler sampler : samplers_high)
					sampler.sample(item.getValue(), item.getKey());
			long end = System.currentTimeMillis();
			System.out.println(population.size() + " population, " + k + " samples, 100 precision\t" + (end - start));
		}
	}
}
