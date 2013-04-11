package util;

public class Parameters
{
	// job configurations
	public static final String	LEFT_PATH					= "dist.left.path";
	public static final String	RIGHT_PATH					= "dist.right.path";
	public static final String	N_SAMPLES					= "dist.sample.size";
	public static final String	MIN_PATTERN_LENGTH			= "dist.min.pattern.length";
	public static final String	MIN_PRECISION				= "dist.min.precision";
	public static final String	MAX_PRECISION				= "dist.max.precision";
	public static final String	MAX_RECORD_LENGTH			= "dist.max.record.length";

	public static final boolean	DEBUG_MODE					= true;

	// predefined values
	public static final int		DEFAULT_MIN_PATTERN_LENGTH	= 0;			// adjustable
	public static final int		DEFAULT_RECORD_LENGTH		= 250;			// adjustable
	public static final int		DEFAULT_MIN_PRECISION		= 20;			// fixed
	public static final int		DEFAULT_MAX_PRECISION		= 100;			// fixed

	// separators
	public final static String	SepItems					= " ";
	public final static String 	SepItemsRegex = " +";
	public final static String	SepIndexes					= ",";
	public final static String	SepIndexRecord				= ",";
	public final static String	SepIndexWeight				= " ";
	public final static String	SepRecords					= ",";
	public final static String  SepFilePosition = "@";


	private Parameters()
	{

	}
}
