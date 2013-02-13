package sample.record.mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apfloat.Apfloat;
import org.apfloat.Apint;
import org.apfloat.FixedPrecisionApfloatHelper;

import rng.RNG;
import setting.PARAMETERS;

public class RecordSamplingMapper extends Mapper<NullWritable, Text, NullWritable, Text>
{
	private int nSamples = 0;

	// instances of A-RES
	private List<ReserviorOneSampler> instances;


	@Override
	public void setup(Context context)
	{
		// get the number of samples
		nSamples = Integer.parseInt(context.getConfiguration().get(PARAMETERS.N_SAMPLES));
		instances = new ArrayList<ReserviorOneSampler>(nSamples);

		for (int i = 0; i < nSamples; i++)
			instances.add(new ReserviorOneSampler());
	}


	@Override
	public void map(NullWritable key, Text value, Context context) throws IOException,
					InterruptedException
	{
		String[] indexweight = value.toString().split(PARAMETERS.SepIndexWeight);
		String index = indexweight[0];
		String weight = indexweight[1];

		for (ReserviorOneSampler sampler : instances)
			sampler.sample(weight, index);
	}


	@Override
	public void cleanup(Context context) throws IOException, InterruptedException
	{
		for (ReserviorOneSampler sampler : instances)
		{
			StringBuilder output = new StringBuilder();
			output.append(sampler.getValue().toString())
							.append(PARAMETERS.SepIndexWeight)
							.append(sampler.getKey());

			context.write(NullWritable.get(), new Text(output.toString()));
		}
	}

	public static class ReserviorOneSampler
	{
		private Apfloat key;
		private Object item;

		private final RNG random;
		private boolean startjump;
		private Apint accumulation;
		private Apfloat Xw;
		
		private int precision;
		FixedPrecisionApfloatHelper helper;

		final static private int defaultPrecision = 10;

		public ReserviorOneSampler()
		{
			this(defaultPrecision);
		}


		// main constructor
		private ReserviorOneSampler(int p)
		{
			precision = p;
			helper = new FixedPrecisionApfloatHelper(precision);
			
			key = null; // minimum value
			item = null; // useless

			random = new RNG();
			accumulation = Apint.ZERO;
			Xw = Apfloat.ZERO;
			startjump = true;
		}


		public String getKey()
		{
			printPrecision("key", key);
			return key.toString(true);
		}


		public Object getValue()
		{
			return item;
		}

		
		// true if sampled, false otherwise
		public boolean sample(String w, Object value)
		{
			final Apint intWeight = new Apint(w);		// used in summing up

			if (key == null) // if the reservoir is not full
			{
				Apfloat floatWeight = new Apfloat(w);
//				printPrecision("fw", floatWeight);

				Apfloat r = new Apfloat(String.valueOf(random.nextDouble()));
//				printPrecision("r", r);
				
				Apfloat exp  = helper.divide(Apfloat.ONE, floatWeight);
//				printPrecision("exp", exp);
				
//				key = helper.pow(r, exp);
				key = pow(r, exp);
//				printPrecision("key", key);
				
				item = value;

				return true;
			}
			else
			// if the reservoir is exhausted
			{
				
				if (startjump)
				{
					double r;
					for (r = random.nextDouble(); (r == 0.0d) || (r == 1.0d); r = random
									.nextDouble());

					Apfloat rand = new Apfloat(String.valueOf(r));
//					printPrecision("rand", rand);
					
//					printPrecision("key", key);
					Xw = helper.log(rand, key);
//					printPrecision("Xw", Xw);
					
					accumulation = Apint.ZERO;
					startjump = false;
				}

				// if skipped
				accumulation = accumulation.add(intWeight);
				
				if (accumulation.compareTo(Xw) >= 0) // no skip
				{
					Apfloat floatWeight = new Apfloat(w);

					Apfloat tw = helper.pow(key, floatWeight);
//					printPrecision("tw", tw);

					Apfloat r = new Apfloat(String.valueOf(random.nextDouble()));
//					printPrecision("r", r);
					
					Apfloat r2 = Apfloat.ONE.subtract(tw).multiply(r).add(tw);
//					printPrecision("r2", r2);
					
					Apfloat exp = helper.divide(Apfloat.ONE, floatWeight);
//					printPrecision("exp", exp);

//					key = helper.pow(r2, exp);
					key = pow(r2, exp);
//					printPrecision("key", key);
					
					item = value;

					startjump = true;
					return true;
				}

				// skip
				return false;
			}
		}
		
		private Apfloat pow(Apfloat x, Apfloat y)
		{
			Apfloat key = helper.pow(x, y);
			while (key.compareTo(Apfloat.ONE) == 0)
			{
				precision ++;
				helper = new FixedPrecisionApfloatHelper(precision);
				key = helper.pow(x, y);
			}
			return key;
		}
		
		public static void printPrecision(String name, Apfloat f)
		{
			System.out.println(name + "\t" + f.precision() + "\t" + f.toString(true));
		}
	}
	
	public static void main(String [] args)
	{
//		Apfloat apfloat = new Apfloat("0.123456789");
//		ReserviorOneSampler.printPrecision("test", apfloat);
//		ReserviorOneSampler.printPrecision("test", Apfloat.ONE.subtract(apfloat));
//		ReserviorOneSampler.printPrecision("test", Apfloat.ONE.divide(apfloat));
//		ReserviorOneSampler.printPrecision("test", Apfloat.ONE.divide(apfloat.precision(5)));
		System.exit(0);
		
		int nPopulation = 10;
		int nSample = 10;
		
		// initialize instances
		List<ReserviorOneSampler> instances = new ArrayList<ReserviorOneSampler>(nSample);
		for (int i = 0; i < nSample; i++)
			instances.add(new ReserviorOneSampler());
		
		// sample, while generating
		for (int i=3; i<=nPopulation; i++)
		{
			// generate
			String key = String.valueOf(i);
			String value = String.valueOf(i);
			
			// sample
			for(ReserviorOneSampler sampler : instances)
				sampler.sample(key, value);
		}
		
		for (ReserviorOneSampler sampler : instances)
			System.out.println(sampler.getValue().toString() + ":\t" + sampler.getKey());
	}
}
