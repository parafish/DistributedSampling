package util;

public class Config
{
	// job configurations
	public static final boolean DEBUG_MODE = true;
	
	// internal configuration
	public static final String LEFT_PATH = "dps.left.path";		// the left path of data set. directory or file.
	public static final String RIGHT_PATH = "dps.right.path";
	public static final String N_SAMPLES = "dps.sample.size";
	public static final String RIGHT_LINE_LENGTH = "dps.right.line.length";
	public static final String LONGEST_LINE_LENGTH = "dps.longest.line.length";
	
	// can be explicitly set
	public static final String MIN_PATTERN_LENGTH = "dps.min.pattern.length";
	public static final String MAX_RECORD_LENGTH = "dps.max.record.length";
	// default values
	public static final int DEFAULT_MIN_PATTERN_LENGTH = 0; // adjustable
	public static final int DEFAULT_MAX_RECORD_LENGTH = 50; // adjustable
	
	public static final int DEFAULT_LEFT_BUFFER_LINES = 1000;	// number of lines that a mapper will read once
		
	// separators
	public final static String SepItems = " ";
	public final static String SepItemsRegex = " +";
	public final static String SepIndexes = ",";
	public final static String SepIndexRecord = ",";
	public final static String SepIndexWeight = " ";
	public final static String SepRecords = ",";
	public final static String SepFilePosition = "@";


	private Config()
	{

	}
}
