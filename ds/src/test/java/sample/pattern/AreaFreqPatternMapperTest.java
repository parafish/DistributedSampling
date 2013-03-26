package sample.pattern;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.thirdparty.guava.common.collect.ConcurrentHashMultiset;
import org.apache.hadoop.thirdparty.guava.common.collect.Multiset;
import org.junit.Test;


public class AreaFreqPatternMapperTest
{

	@Test
	public void test()
	{}
	
	
	@Test
	public static void testWeightedSampling()
	{
		String record = "1 2 3 4";
		int k = 10000;
		AreaFreqPatternMapper mapper = new AreaFreqPatternMapper();
		Multiset<String> stats = ConcurrentHashMultiset.create();
		
		for (int i =0; i<k; i++)
		{
			List<String> items = new ArrayList<String>(Arrays.asList(record.split(" ")));
			StringBuilder builder = new StringBuilder();
			for (String s : mapper.sampleWeighted(items))
				builder.append(s);
			stats.add(builder.toString());
			
		}

		System.out.println("sample: " + k);
		Map<Double, String> result = new TreeMap<Double, String>();
		for (String item : stats.elementSet())
			result.put(stats.count(item) * 1.0d / k, item);
		for (Map.Entry<Double, String> entry : result.entrySet())
			System.out.println(entry.getValue() + "\t" + entry.getKey());
	}
	
	public static void main(String [] args)
	{
		testWeightedSampling();
	}

}