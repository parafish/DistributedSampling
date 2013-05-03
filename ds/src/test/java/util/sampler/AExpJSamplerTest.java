package util.sampler;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

public class AExpJSamplerTest
{

	@Test
	public void test()
	{

		Map<String, Long> items = new HashMap<String, Long>();
		items.put("weight10", 10L);
		items.put("weight20", 20L);
		items.put("weight30", 30L);

		int k = 2000;
		List<Sampler> samplers = new ArrayList<Sampler>(k);
		for (int i = 0; i < k; i++)
			samplers.add(new AExpJSampler<String>());

		for (Map.Entry<String, Long> entry : items.entrySet())
			for (Sampler<String> sampler : samplers)
				sampler.sample(entry.getKey(), (double)entry.getValue());

		for (Sampler<String> sampler : samplers)
			System.out.println(sampler.getItem());

	}

}
