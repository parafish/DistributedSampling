package sample.record;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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
		int n = 1000; // 1000 samples
		
		Map<String, String> population = new TreeMap<String, String>();
		population.put("item1", "8");
		population.put("item2", "4");
//		population.put("item3", "2000");
		
		List<ReserviorOneSampler> samplers = new ArrayList<ReserviorOneSampler>(n);
		
		for (int i=0; i< n; i++)
			samplers.add(new ReserviorOneSampler());
		
		for (Map.Entry<String, String> item : population.entrySet())
			for (ReserviorOneSampler sampler : samplers)
				sampler.sample(item.getValue(), item.getKey());
		
		Multiset<String> stats = ConcurrentHashMultiset.create();
		
		for (ReserviorOneSampler sampler : samplers)
			stats.add((String) sampler.getItem());
		
		System.out.println("Population: " + n);
		for (String item : stats.elementSet())
			System.out.println(item + "\t" + stats.count(item));
		
		float error = stats.count("item1") * 1.0f / stats.count("item2") - 1.0f/3;
		
		
//		assertTrue("see the proportion", Math.abs(error) < 0.05f);
	}
}
