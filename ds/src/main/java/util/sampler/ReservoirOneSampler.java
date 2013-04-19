package util.sampler;

import static util.Config.DEBUG_MODE;

import java.util.logging.Logger;

import org.apfloat.Apfloat;
import org.apfloat.ApfloatMath;
import org.apfloat.Apint;
import org.apfloat.FixedPrecisionApfloatHelper;

import sample.record.RecordSamplingMapper;
import util.Config;
import util.RNG;


/**
 * 
 * @author zheyi
 * 
 */
public class ReservoirOneSampler
{
	private final static Logger LOGGER = Logger.getLogger(RecordSamplingMapper.class.getName());

	private Apfloat key = null;
	private Object item = null;

	private final RNG random = new RNG();
	private boolean startjump = true;
	private Apint accumulation = Apint.ZERO;
	private Apfloat Xw = Apfloat.ZERO;

	private static int precision;
	private static FixedPrecisionApfloatHelper helper;

	// XXX: make it changeable
	public final static int defaultPrecision = Config.DEFAULT_MIN_PRECISION;
	public final static int maximumPrecision = Config.DEFAULT_MAX_PRECISION;


	/**
	 * Constructs a reservoir sampling algorithm instance with the default
	 * precision
	 */
	public ReservoirOneSampler()
	{
		this(defaultPrecision);
	}


	/**
	 * Initializes the reservoir sampling algorithm with an initial precision
	 * This precision will be increased if needed.
	 * 
	 * @param p the initial precision
	 */
	public ReservoirOneSampler(int p)
	{
		precision = p;
		helper = new FixedPrecisionApfloatHelper(precision);
	}


	/**
	 * Decides if an item would be sampled with its weight.
	 * <p>
	 * 
	 * @param w the weight of this item. should be an integer represented in
	 *            <code>String</code>.
	 * @param obj the item to decide
	 * @return if the item was sampled
	 */
	public boolean sample(String w, Object obj)
	{
		if (key == null) // if the reservoir is not full
		{
			Apfloat floatWeight = new Apfloat(w);
			if (floatWeight.compareTo(Apfloat.ZERO) <= 0)
				return false;
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
				Apfloat tw = null;
				try
				{
					tw = helper.pow(key, floatWeight);
				}
				catch (ArithmeticException e)
				{
					if (DEBUG_MODE)
						LOGGER.severe("pow(x, y) with precision " + precision + ": x=" + key.toString(true) + " y="
										+ floatWeight.toString(true));
					e.printStackTrace();
					// TODO: add counters
					tw = Apfloat.ZERO.precision(precision);
				}
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
	 * Calculates the power x^y. If the precision is not enough, it will be
	 * increased until reaching the maximum. If it reaches the maximum, a
	 * closest float will be returned.
	 * <p>
	 * For example, x^y = 0.9999995, but the precision now is 5, so under this
	 * precision x^y would be 1. We can increase the precision by 5, so the
	 * right answer will be returned. However, if the maximum precision is 5,
	 * 0.99999 will be returned.
	 * 
	 * @param x
	 * @param y
	 * @return x^y
	 */
	private Apfloat pow(Apfloat x, Apfloat y)
	{
		Apfloat key = helper.pow(x, y);
		while (key.compareTo(Apfloat.ONE) == 0)
		{
			if (precision >= maximumPrecision)
			{
				key = Apfloat.ONE.subtract(ApfloatMath.pow(new Apfloat("0.1"), precision));
				break;
			}

			synchronized (this.helper)
			{
				precision += 5;
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