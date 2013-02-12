package sample.record.mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mrunit.types.Pair;
import org.apfloat.Apfloat;
import org.apfloat.ApfloatMath;
import org.apfloat.Apint;

import rng.RNG;
import setting.PARAMETERS;

public class RecordSamplingMapper extends Mapper<NullWritable, Text, NullWritable, Text>
{
	private int nSamples = 0;

	// instances of A-RES
	private List<ReserviorSampler> instances;

	@Override
	public void setup(Context context)
	{
		// get the number of samples
		nSamples = Integer.parseInt(context.getConfiguration().get(PARAMETERS.N_SAMPLES));
		instances = new ArrayList<ReserviorSampler>(nSamples);

		for (int i = 0; i < nSamples; i++)
			instances.add(new ReserviorSampler(1)); // only 1, due to
													// replacement
	}


	@Override
	public void map(NullWritable key, Text value, Context context) throws IOException,
					InterruptedException
	{
		String[] indexweight = value.toString().split(PARAMETERS.SepIndexWeight);
		String index = indexweight[0];
		String weight = indexweight[1];

		// scan n reservoirs
		for (ReserviorSampler sampler : instances)
		{
			sampler.sample(weight, index);
		}
	}


	@Override
	public void cleanup(Context context) throws IOException, InterruptedException
	{
		for (ReserviorSampler sampler : instances)
		{
			Pair<Apfloat, Object> pair = sampler.getReservior().poll();
			context.write(NullWritable.get(), new Text(pair.getSecond() + PARAMETERS.SepIndexWeight
							+ pair.getFirst().toString(true)));
		}
	}

	public static class ReserviorSampler
	{
		private final int nSample;
		private final PriorityQueue<Pair<Apfloat, Object>> reservior;
		private final RNG random;
		private final int precision;
		private boolean startjump;
		private Apint accumulation;
		private Apfloat Xw;


		public ReserviorSampler(int n)
		{
			// with default precision = 100
			this(n, 60);
		}


		public ReserviorSampler(int n, int p)
		{
			precision = p;
			if (n <= 0) throw new RuntimeException("The size of reservior cannot be zero");
			nSample = n;
			reservior = new PriorityQueue<Pair<Apfloat, Object>>(n,
							new Comparator<Pair<Apfloat, Object>>()
							{
								@Override
								public int compare(Pair<Apfloat, Object> o1,
												Pair<Apfloat, Object> o2)
								{
									return o1.getFirst().compareTo(o2.getFirst());
								}
							});
			random = new RNG();
			accumulation = Apint.ZERO;
			Xw = Apfloat.ZERO;
			startjump = true;
		}


		public PriorityQueue<Pair<Apfloat, Object>> getReservior()
		{
			return reservior;
		}


		public boolean sample(String w, Object value)
		{
			final Apint intWeight = new Apint(w);
			final Apfloat floatWeight = new Apfloat(w);// , precision);

			if (reservior.size() < nSample) // if the reservoir is not full
			{
				Apfloat r = new Apfloat(String.valueOf(random.nextDouble()), precision);
				Apfloat exp = Apfloat.ONE.divide(floatWeight);

				Apfloat key = ApfloatMath.pow(r, exp).precision(precision);

				// System.out.println("First, r : " + r.toString(true));
				// System.out.println("First, weight: " +
				// floatWeight.toString(true));
				// System.out.println("First, exp : " + exp.toString(true));
				// System.out.println("First, key : " + key.toString(true));

				reservior.add(new Pair<Apfloat, Object>(key, value));

				return true;
			}
			else
			// if the reservoir is exhausted
			{
				if (startjump)
				{
					double r = 0.0d;
					while (r == 0.0d || r == 1.0d)
						r = random.nextDouble(); // r must not be zero or one

					Apfloat rand = new Apfloat(String.valueOf(r), precision);
					Apfloat Tw = reservior.peek().getFirst();

					Xw = ApfloatMath.log(rand).divide(ApfloatMath.log(Tw));

					accumulation = Apint.ZERO;
					startjump = false;
				}

				// if skipped
				accumulation = accumulation.add(intWeight);

				if (accumulation.compareTo(Xw) >= 0) // no skip
				{
					// TODO: is it a workaround for the problem in
					// ApfloatMath.pow?
					Apfloat min = reservior.poll().getFirst(); // delete the
																// minimum
																// System.out.println("min: "
																// +
																// min.toString(true));
					// TODO: ApfloatMath.pow is problematic
					// System.out.println("min: " + min.toString(true)
					// +"\tp: "+min.precision());
					// System.out.println("floatweight: " +
					// floatWeight.toString(true)
					// +"\tp: "+floatWeight.precision());
					Apfloat tw = null;
					try
					{
						// taylor expanasion, (1-x)^n ~ 1-nx+O(x^2)
//						Apfloat x = new Apfloat("1", Apfloat.INFINITE).subtract(min);
//						Apfloat nx = x.multiply(floatWeight);
//						Apfloat level2 = floatWeight
//										.subtract(new Apfloat("1", Apfloat.INFINITE))
//										.multiply(floatWeight)
//										.multiply(ApfloatMath.pow(x, 2))
//										.divide(new Apfloat("2", precision));

//						tw = new Apfloat("1", Apfloat.INFINITE).subtract(nx).add(level2);

//						System.out.println("x:  " + x.toString(true));
//						System.out.println("fl: " + floatWeight.toString(true));
//						System.out.println("nx: " + nx.toString(true));
//						System.out.println("l2: " + level2.toString(true));
//						System.out.println("tw: " + tw.toString(true));

						tw = ApfloatMath.pow(min, floatWeight).precision(precision);
					}
					catch (ArithmeticException e)
					{
						// System.err.println("min\t\t (" + min.precision() +
						// "): " + min.toString(true));
						// System.err.println("floatWeight\t (" +
						// floatWeight.precision() + "): " +
						// floatWeight.toString(true));
						// System.err.println(e.getMessage());
						// FIXME: not true!!!

						throw e;
					}

					Apfloat r = new Apfloat(String.valueOf(random.nextDouble()), precision);

					Apfloat r2 = new Apfloat("1", precision).subtract(tw).multiply(r).add(tw);
					//
					// if the minimum key in the reservor = 1.0d,
					// then Xw=-Infinity, tw=1.0d,
					// therefore random.nextDouble < tw holds forever...
					// leads to the for-loop below infinite.
					Apfloat exp = Apfloat.ONE.divide(floatWeight);

					Apfloat key = ApfloatMath.pow(r2, exp).precision(precision);

					reservior.add(new Pair<Apfloat, Object>(key, value));

					startjump = true;
					return true;
				}

				// skip
				return false;
			}
		}
	}
}
