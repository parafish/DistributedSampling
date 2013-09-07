package edu.tue.cs.capa.dps.disc;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.thirdparty.guava.common.collect.Sets;

import edu.tue.cs.capa.dps.util.Config;
import edu.tue.cs.capa.dps.util.DpsCounters;
import edu.tue.cs.capa.dps.util.DpsExceptions.MissingParameterException;
import edu.tue.cs.capa.dps.util.Helper;
import edu.tue.cs.capa.dps.util.sampler.DryRunSampler;



public class DiscMapper extends MapReduceBase implements Mapper<Writable, Text, DoubleWritable, Text>
{
	private int nSample;

	private int maxRecordLength;
	//	private int rightBufferLines;

	private int rightLineLength;

	private String leftFile;
	private String rightDir;
	
	private String delimiter;

	private OutputCollector<DoubleWritable, Text> output;
	private Reporter reporter;
	private FileSystem fs;

	private Map<Path, Long> rightPaths = new HashMap<Path, Long>();

	private List<DryRunSampler<String>> instances;


	@Override
	public void configure(JobConf jobConf)
	{
		leftFile = jobConf.get("map.input.file");
		rightDir = jobConf.get(Config.RIGHT_PATH);
		System.out.println("Left file path: " + leftFile);
		System.out.println("Right file path: " + rightDir);

		maxRecordLength = jobConf.getInt(Config.MAX_RECORD_LENGTH, Config.DEFAULT_MAX_RECORD_LENGTH);
		rightLineLength = jobConf.getInt(Config.RIGHT_LINE_LENGTH, 0);
		nSample = jobConf.getInt(Config.N_SAMPLES, 0);

		if (nSample == 0)
			throw new MissingParameterException("The sample size is not set");
		if (rightLineLength == 0)
			throw new MissingParameterException("Missing parameter: rightLineLength");

		instances = new ArrayList<DryRunSampler<String>>(nSample);
		for (int i = 0; i < nSample; i++)
			instances.add(new DryRunSampler<String>());

		System.out.println("Max record length: " + maxRecordLength);

		delimiter = jobConf.get(Config.ITEM_DELIMITER, Config.DEFAULT_ITEM_DELIMITER);
		delimiter += "+";
		System.out.println("Item delimiter: " + delimiter);
		
		try
		{
			fs = FileSystem.get(jobConf);
			Path rightPath = new Path(rightDir);
			FileStatus rightPathStatus = fs.getFileStatus(rightPath);
		
			if ( ! rightPathStatus.isDir())
			{
				rightPaths.put(rightPathStatus.getPath(), rightPathStatus.getLen() / rightLineLength);
			}
			else
			{
				for (FileStatus fileStatus : fs.listStatus(rightPath, new PathFilter() {
					@Override
					public boolean accept(Path path) {	return path.getName().startsWith("part");	}
					}))
				{
					if (!fileStatus.isDir() && fileStatus.getLen() > 0)
						rightPaths.put(fileStatus.getPath(), fileStatus.getLen() / rightLineLength);
				}
			}
		}
		catch (IOException e)
		{
			System.err.println("IO Exception when reading right file list");
			throw new RuntimeException(e);
		}

	}


	@Override
	public void map(Writable key, Text value, OutputCollector<DoubleWritable, Text> output, Reporter reporter)
					throws IOException
	{
		String leftKey = leftFile + Config.SepFilePosition + key.toString();
		Set<String> leftRecord = Sets.newHashSet(value.toString().trim().split(delimiter));

		int exp = leftRecord.size() > maxRecordLength ? maxRecordLength : leftRecord.size();
		double leftWeight = Math.pow(2, exp);

		for (Map.Entry<Path, Long> rp : rightPaths.entrySet()) // iterate all the right files
		{
			// open this file
			for (long i = 0; i < rp.getValue(); i++)
			{
				reporter.incrCounter(DpsCounters.RECORD_TUPLES, 1);
				boolean unread = true;
				long offset = rightLineLength * i;

				for (DryRunSampler<String> sampler : instances)
				{
					reporter.incrCounter(DpsCounters.TOTAL_DRY_RUN_TIMES, 1);
					if (sampler.dryRun(leftWeight))
					{
						unread = false;
						reporter.incrCounter(DpsCounters.SUCC_DRY_RUN_TIMES, 1);
						// read, then sample
						Set<String> rightRecord = Helper.readRecordAsSet(fs, rp.getKey(), offset, delimiter);
						int intersect = Sets.intersection(leftRecord, rightRecord).size();
						double weight = leftWeight - Math.pow(2, intersect);

						StringBuilder index = new StringBuilder()
													.append(leftKey)
													.append(Config.SepIndexes)
													.append(rp.getKey().toString())
													.append(Config.SepFilePosition)
													.append(offset);
						if (sampler.sample(index.toString(), weight))
						{
							reporter.incrCounter(DpsCounters.SUCC_SAMPLE_TIMES, 1);
						}
					}
				}

				if (unread)
					reporter.incrCounter(DpsCounters.SKIPPED_TUPLES, 1);

				if (i % 10000 == 0)
				{
					reporter.progress();
				}
			}
		}

		this.output = output;
		this.reporter = reporter;
	}


	@Override
	public void close() throws IOException
	{
		for (DryRunSampler<String> sampler : instances)
		{
			try
			{
				this.output.collect(new DoubleWritable(sampler.getKey()), new Text(sampler.getItem()));
				this.reporter.incrCounter(DpsCounters.OVERFLOWED_TIMES, sampler.getOverflowed());
			}
			catch (NullPointerException exception)
			{
				System.err.println("Nothing in sampler " + instances.indexOf(sampler));
			}
		}
	}

}
