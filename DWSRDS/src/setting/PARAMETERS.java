package setting;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

public class PARAMETERS
{
	public static enum  SeminarCounters
	{
		MALFORMED
	}

	
	// job configurations
	public static final String LEFT_PATH = "dist.left.path";
	public static final String RIGHT_PATH = "dist.right.path";
	public static final String N_SAMPLES = "dist.sample.size";
	public static final String MAX_LENGTH = "dist.record.max";
	
	// separators
	public final static String SepItems = " ";
	public final static String SepIndexes = ",";
	public final static String SepIndexRecord = ",";
	public final static String SepIndexWeight = " ";
	public final static String SepRecords = ",";
	
	// paths, for testing only
	public static final Path localInputPath = new Path("/home/zheyi/sampling/data/mushroom_pos.dat");
	public static final Path localInputPath2 = new Path("/home/zheyi/sampling/data/mushroom_neg.dat");
	public static final Path localOutputPath = new Path("/home/zheyi/sampling/output");
	public static final Path localTempPath = new Path("/home/zheyi/sampling/temp");
	
	public static final Path pseudoInputPath = new Path("input/iris.dat");
	public static final Path pseudoOutputPath = new Path("output");
	public static final Path pseudoTempPath = new Path("temp");
	
	// helpers, for testing only
	Configuration localConf = new Configuration();
	
	public static Configuration getLocalConf()
	{
		Configuration conf = new Configuration();
		conf.set("fs.default.name", "file:///");
		conf.set("mapred.job.tracker", "local");
		return conf;
	}
	
	public static Configuration getPseudoConf()
	{
		Configuration conf = new Configuration();
		conf.set("fs.default.name", "hdfs://localhost:9000");
		conf.set("mapred.job.tracker", "localhost:9001");
		
		return conf;
	}
}
