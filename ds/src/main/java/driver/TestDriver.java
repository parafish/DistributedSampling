package driver;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

import pre.AreaFreqMapper;


public class TestDriver
{
	public static void main(String[] args) throws IOException
	{
		if (args.length != 2)
		{
			System.err.println("Usage: <input path> <output path>");
			System.exit(-1);
		}
		
		JobConf conf = new JobConf(TestDriver.class);
		conf.setJobName("line count, test");
		
		
		FileInputFormat.addInputPath(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		
		conf.setMapperClass(AreaFreqMapper.class);
		conf.setNumReduceTasks(0);
		
		JobClient.runJob(conf);

	}

}
