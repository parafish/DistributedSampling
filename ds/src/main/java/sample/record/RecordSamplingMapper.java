package sample.record;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apfloat.Apfloat;
import org.apfloat.ApfloatMath;
import org.apfloat.Apint;
import org.apfloat.FixedPrecisionApfloatHelper;

import util.Parameters;
import util.RNG;


/**
 * Samples from a stream using reservoir sampling.
 * <p>
 * If <em>k</em> samples are needed, there will be <em>k</em> instances of the
 * reservoir sampling algorithm, each of which maintains a reservoir of size 1.
 * 
 * @author zheyi
 * 
 */
public class RecordSamplingMapper extends MapReduceBase implements Mapper<Writable, Text, NullWritable, Text>
{
	// instances of A-RES
	private List<ReserviorOneSampler>			instances	= null;
	// output collector
	private OutputCollector<NullWritable, Text>	output		= null;


	/**
	 * Initialized <code>N_SAMPLES</code> instances of the reservoir sampling algorithm
	 */
	@Override
	public void configure(JobConf jobConf)
	{
		// get the number of samples
		int nSamples = Integer.parseInt(jobConf.get(Parameters.N_SAMPLES));
		instances = new ArrayList<ReserviorOneSampler>(nSamples);

		for (int i = 0; i < nSamples; i++)
			instances.add(new ReserviorOneSampler());
	}


	/**
	 * Decides if an incoming key/value pair should be sampled. The <code>value</code> is the 
	 * the weight, in integer.
	 */
	@Override
	public void map(Writable key, Text value, OutputCollector<NullWritable, Text> output, Reporter reporter)
	{
		for (ReserviorOneSampler sampler : instances)
			sampler.sample(value.toString(), key.toString());

		this.output = output;
	}


	/**
	 * Emits the sampled key/value pairs.
	 */
	@Override
	public void close() throws IOException
	{
		for (ReserviorOneSampler sampler : instances)
		{
			StringBuilder output = new StringBuilder();
			output.append(sampler.getItem().toString()).append(Parameters.SepIndexWeight).append(sampler.getKey());

			this.output.collect(NullWritable.get(), new Text(output.toString()));
		}
	}


	/**
	 * 
	 * @author zheyi
	 * 
	 */
	static class ReserviorOneSampler
	{
		private Apfloat								key					= null;
		private Object								item				= null;

		private final RNG							random				= new RNG();
		private boolean								startjump			= true;
		private Apint								accumulation		= Apint.ZERO;
		private Apfloat								Xw					= Apfloat.ZERO;

		private static int							precision;
		private static FixedPrecisionApfloatHelper	helper;

		public final static int					defaultPrecision	= 20;
		public final static int					maximumPrecision	= 100;


		/**
		 * Constructs a reservoir sampling algorithm instance with the default
		 * precision
		 */
		public ReserviorOneSampler()
		{
			this(defaultPrecision);
		}


		/**
		 * Initializes the reservoir sampling algorithm with an initial
		 * precision This precision will be increased if needed.
		 * 
		 * @param p
		 *            the initial precision
		 */
		public ReserviorOneSampler(int p)
		{
			precision = p;
			helper = new FixedPrecisionApfloatHelper(precision);
		}


		/**
		 * Decides if an item would be sampled with its weight.
		 * <p>
		 * 
		 * @param w
		 *            the weight of this item. should be an integer represented
		 *            in <code>String</code>.
		 * @param obj
		 *            the item to decide
		 * @return if the item was sampled
		 */
		public boolean sample(String w, Object obj)
		{
			if (key == null) // if the reservoir is not full
			{
				Apfloat floatWeight = new Apfloat(w);
				Apfloat r = random.nextApfloat(precision);
				Apfloat exp = helper.divide(Apfloat.ONE, floatWeight);
				key = pow(r, exp);
				item = obj;
				return true;
			}
			else
			// if the reservoir is exhausted
			{
				if (startjump)
				{
					Apfloat r = random.nextApfloat(precision);
					Xw = helper.log(r, key);
					accumulation = Apint.ZERO;
					startjump = false;
				}

				// if skipped
				accumulation = accumulation.add(new Apint(w));

				if (accumulation.compareTo(Xw) >= 0) // no skip
				{
					Apfloat floatWeight = new Apfloat(w);
					Apfloat tw = helper.pow(key, floatWeight);
					Apfloat r2 = random.nextApfloat(tw, Apfloat.ONE, helper);
					Apfloat exp = helper.divide(Apfloat.ONE, floatWeight);
					key = pow(r2, exp);
					item = obj;
					startjump = true;
					return true;
				}

				return false; // skip
			}
		}


		/**
		 * Calculates the power x^y. If the precision is not enough, it will be increased
		 * until reaching the maximum. If it reaches the maximum, a closest float will be returned.
		 * <p>For example, x^y = 0.9999995, but the precision now is 5, so under this precision
		 * x^y would be 1. We can increase the precision by 5, so the right answer will be returned.
		 * However, if the maximum precision is 5, 0.99999 will be returned.
		 * @param x
		 * @param y
		 * @return
		 */
		private Apfloat pow(Apfloat x, Apfloat y)
		{
			Apfloat key = helper.pow(x, y);
			while (key.compareTo(Apfloat.ONE) == 0)
			{
				if (precision == maximumPrecision)
				{
					key = Apfloat.ONE.subtract(ApfloatMath.pow(new Apfloat("0.1"), precision));
					break;
				}

				precision += 5;
				synchronized (this.helper)
				{
					helper = new FixedPrecisionApfloatHelper(precision);	
				}
				key = helper.pow(x, y);
			}
			return key;
		}


		public static int getPrecision()
		{
			return precision;
		}


		public String getKey()
		{
			return key.toString(true);
		}


		public Object getItem()
		{
			return item;
		}


		public static void printPrecision(String name, Apfloat f)
		{
			System.out.println(name + "\t" + f.precision() + "\t" + f.toString(true));
		}
	}
}
