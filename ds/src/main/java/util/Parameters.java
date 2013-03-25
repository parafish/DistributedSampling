package util;

public class Parameters
{
	// job configurations
	public static final String	LEFT_PATH			= "dist.left.path";
	public static final String	RIGHT_PATH			= "dist.right.path";
	public static final String	N_SAMPLES			= "dist.sample.size";
	public static final String	MIN_PATTERN_LENGTH	= "dist.min.pattern.length";
	public static final String	MIN_PRECISION		= "dist.min.precision";
	public static final String	MAX_PRECISION		= "dist.max.precision";
	public static final String	MAX_RECORD_LENGTH	= "dist.max.record.length";

	public static boolean		DEBUG_MODE			= true;

	// separators
	public final static String	SepItems			= " ";
	public final static String	SepIndexes			= ",";
	public final static String	SepIndexRecord		= ",";
	public final static String	SepIndexWeight		= " ";
	public final static String	SepRecords			= ",";


	private Parameters()
	{

	}
}
