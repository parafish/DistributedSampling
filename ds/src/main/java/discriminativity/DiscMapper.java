package discriminativity;

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
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.thirdparty.guava.common.collect.Sets;

import util.Config;
import util.DpsCounters;
import util.DpsExceptions.MissingParameterException;
import util.Helper;
import util.sampler.DryRunSampler;


public class DiscMapper extends MapReduceBase implements Mapper<Writable, Text, Writable, Text>
{
	private static final Log LOG = LogFactory.getLog(DiscMapper.class);

	private int nSample;

	private int maxRecordLength;
	private int leftBufferLines;
	//	private int rightBufferLines;

	private int rightLineLength;

	private String leftFile;
	private String rightDir;

	private OutputCollector<Writable, Text> output;
	private Reporter reporter;
	private FileSystem fs;

	private int leftLineCount = 0;
	private Map<String, Set<String>> leftRecords = new HashMap<String, Set<String>>(); // buffer

	private Map<Path, Long> rightPaths = new HashMap<Path, Long>();

	private List<DryRunSampler<String>> instances = null;

	@Override
	public void configure(JobConf jobConf)
	{
		leftFile = jobConf.get("map.input.file");
		rightDir = jobConf.get(Config.RIGHT_PATH);
		LOG.info("Left file path: " + leftFile);
		LOG.info("Right file path: " + rightDir);
		
		maxRecordLength = jobConf.getInt(Config.MAX_RECORD_LENGTH, Config.DEFAULT_MAX_RECORD_LENGTH);
		leftBufferLines = jobConf.getInt(Config.LEFT_BUFFER_LINES, Config.DEFAULT_LEFT_BUFFER_LINES);
		rightLineLength = jobConf.getInt(Config.RIGHT_LINE_LENGTH, 0); 
		
		nSample = jobConf.getInt(Config.N_SAMPLES, 0);
		
		if (nSample == 0)
			throw new MissingParameterException("The sample size is not set");
		if (rightLineLength == 0)
			throw new MissingParameterException("Missing parameter: rightLineLength");

		instances = new ArrayList<DryRunSampler<String>>(nSample);
		for (int i = 0; i < nSample; i++)
			instances.add(new DryRunSampler<String>());


		LOG.info("Max record length: " + maxRecordLength);
		LOG.info("Size of sample: " + nSample);

		try
		{
			fs = FileSystem.get(jobConf);
			Path rightPath = new Path(rightDir);
			FileStatus rightDirStatus = fs.getFileStatus(new Path(rightDir));
			if (!rightDirStatus.isDir())
			{
				rightPaths.put(rightDirStatus.getPath(), rightDirStatus.getLen() / rightLineLength);
			}
			else
			{
				for (FileStatus fileStatus : fs.listStatus(rightPath))
					if (!fileStatus.isDir() && fileStatus.getLen() > 0)
						rightPaths.put(fileStatus.getPath(), fileStatus.getLen() / rightLineLength);
			}
		}
		catch (IOException e)
		{
			LOG.error("IO Exception when reading right file list");
			throw new RuntimeException(e);
		}

	}


	@Override
	public void map(Writable key, Text value, OutputCollector<Writable, Text> output, Reporter reporter)
					throws IOException
	{
		String leftKey = leftFile + Config.SepFilePosition + key.toString();
		Set<String> leftValue = Sets.newHashSet(value.toString().trim().split(Config.SepItemsRegex));
		leftRecords.put(leftKey, leftValue);
		leftLineCount++;

		// if left buffer is full
		if (leftLineCount > leftBufferLines)
		{
			joinRightFull();
			leftLineCount = 0;
			leftRecords.clear();
		}

		this.output = output;
		this.reporter = reporter;
	}


	@Override
	public void close() throws IOException
	{
		// last time, do not forget!
		joinRightFull();

		for (DryRunSampler<String> sampler : instances)
		{
			StringBuilder output = new StringBuilder();
			try
			{
				output.append(sampler.getItem().toString()).append(Config.SepIndexWeight).append(sampler.getKey());
			}
			catch (NullPointerException exception)
			{
				System.out.println("Nothing in the sampler");
			}
			this.output.collect(NullWritable.get(), new Text(output.toString()));
		}
	}


	/**
	 * Joins the buffered record from input(left) and all the records from the
	 * right.
	 * 
	 * @return if this join was successful
	 * @throws IOException
	 */
	public boolean joinRightFull() throws IOException
	{
		System.out.println("left record buffer size: " + leftRecords.size());
		for (Map.Entry<String, Set<String>> left : leftRecords.entrySet())
		{
			double leftWeight = Math.pow(2, left.getValue().size());
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
							Set<String> rightRecord = Helper.readRecordAsSet(fs, rp.getKey(), offset);
							int intersect = Sets.intersection(left.getValue(), rightRecord).size();
							double weight = leftWeight - Math.pow(2, intersect);
							
							//String rightIndex = rp.getKey().toString() + Config.SepFilePosition + offset;
							//String combinedIndex = left.getKey() + Config.SepIndexes + rightIndex;

							if (sampler.sample(" ", weight))
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
		}
		return true;
	}
}
