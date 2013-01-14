package sample.record.mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Random;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mrunit.types.Pair;

import setting.PARAMETERS;

public class RecordSamplingMapper extends Mapper<NullWritable, Text, NullWritable, Text>
{
	private int nSamples = 0;

	// instances of algorithms
	List<ReserviorSampler> instances;


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

		// TODO: change it to a 'big' version
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
			Pair<Double, Object> pair = sampler.getReservior().peek();

			context.write(NullWritable.get(), new Text(pair.getSecond() + PARAMETERS.SepIndexWeight
							+ pair.getFirst()));
		}
	}

	public static class ReserviorSampler
	{
		private final int nSample;
		private final PriorityQueue<Pair<Double, Object>> reservior;
		private final Random random;
		private boolean startjump;
		private double accumulation;
		private double Xw;


		public ReserviorSampler(int n)
		{
			nSample = n;
			reservior = new PriorityQueue<Pair<Double, Object>>(n,
							new Comparator<Pair<Double, Object>>()
							{
								@Override
								public int compare(Pair<Double, Object> o1, Pair<Double, Object> o2)
								{
									return Double.compare(o1.getFirst(), o2.getFirst());
								}
							});
			random = new Random();
			accumulation = 0.0d;
			Xw = 0.0d;
			startjump = true;
		}


		public PriorityQueue<Pair<Double, Object>> getReservior()
		{
			return reservior;
		}


		public boolean sample(String weight, Object value)
		{
			final double dweight = Double.parseDouble(weight);

			if (reservior.size() < nSample) // if the reservoir is not full
			{
				double key = Math.pow(random.nextDouble(), 1.0d / dweight);
				reservior.add(new Pair<Double, Object>(key, value));
				return true;
			}
			else
			// if the reservoir is exhausted
			{
				if (startjump)
				{
					double r = random.nextDouble();
					// XXX: note Xw might be -Infinity
					Xw = Math.log(r) / Math.log(reservior.peek().getFirst());
					accumulation = 0.0d;
					startjump = false;
				}

				// if skipped
				accumulation += dweight;

				if (accumulation >= Xw) // no skip
				{
					double mini = reservior.poll().getFirst(); // delete the
																// minimum
					double tw = Math.pow(mini, dweight);
					double r2;

					// FIXME: if the minimum key in the reservor = 1.0d,
					// then Xw=-Infinity, tw=1.0d,
					// therefore random.nextDouble < tw holds forever...
					// leads to the for-loop below infinite.
					if (tw >= 1.0d)
						r2 = 1.0d;
					else
						for (r2 = -1; r2 < tw; r2 = random.nextDouble());

					double key = Math.pow(r2, 1.0d / dweight);

					reservior.add(new Pair<Double, Object>(key, value));
					startjump = true;
					return true;
				}

				// skip
				return false;
			}
		}
	}
}
