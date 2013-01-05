package setting;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

public class PARAMETERS
{
	public static Path localInputPath = new Path("/home/zheyi/sampling/data/iris.dat");
	public static Path localOutputPath = new Path("/home/zheyi/sampling/output");
	public static Path localTempPath = new Path("/home/zheyi/sampling/temp");
	public static Path localTotalweightPath = new Path("/home/zheyi/sampling/totalweight");
	
	public static Path pseudoInputPath = new Path("input/iris.dat");
	public static Path pseudoOutputPath = new Path("output");
	public static Path pseudoTempPath = new Path("temp");
	

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
