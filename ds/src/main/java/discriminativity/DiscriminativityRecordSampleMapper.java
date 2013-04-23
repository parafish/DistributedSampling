package discriminativity;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
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
import util.sampler.DoubleDryRun;
import discriminativity.DspExceptions.MissingParameterException;


public class DiscriminativityRecordSampleMapper extends MapReduceBase implements Mapper<Writable, Text, Writable, Text>
{
	private static final Log LOG = LogFactory.getLog(DiscriminativityDriver.class);

	private int nSample;

	private int maxRecordLength;
	private int leftBufferLines;
	//	private int rightBufferLines;

	private int rightLineLength;

	private String leftFile;
	private String rightDir;

	private JobConf jobConf;
	private OutputCollector<Writable, Text> output;
	private Reporter reporter;
	private FileSystem fs;

	private int leftLineCount = 0;
	private Map<String, Set<String>> leftRecords = new HashMap<String, Set<String>>(); // buffer

	private Map<Path, Long> rightPaths = new HashMap<Path, Long>();

	private List<DoubleDryRun> instances = null;

	private int skipped = 0;
	private long count = 0;
	private int succ = 0;


	@Override
	public void configure(JobConf jobConf)
	{
		this.jobConf = jobConf;

		leftFile = jobConf.get("map.input.file");
		rightDir = jobConf.get(Config.RIGHT_PATH);
		System.out.println("left file path: " + leftFile);
		System.out.println("right file path: " + rightDir);
		maxRecordLength = jobConf.getInt(Config.MAX_RECORD_LENGTH, Config.DEFAULT_MAX_RECORD_LENGTH);
		leftBufferLines = jobConf.getInt(Config.LEFT_BUFFER_LINES, Config.DEFAULT_LEFT_BUFFER_LINES);
		rightLineLength = jobConf.getInt(Config.RIGHT_LINE_LENGTH, 0); // if 0, auto-detect
		nSample = jobConf.getInt(Config.N_SAMPLES, 0);

		instances = new ArrayList<DoubleDryRun>(nSample);

		for (int i = 0; i < nSample; i++)
			instances.add(new DoubleDryRun());

		if (rightLineLength == 0)
			throw new MissingParameterException("Missing parameter: rightLineLength");

		LOG.info("Max record length: " + maxRecordLength);
		LOG.info("File path: " + leftFile);
		LOG.info("Size of sample: " + nSample);

		try
		{
			fs = FileSystem.get(jobConf);
			Path rightPath = new Path(rightDir);
			FileStatus rightDirStatus = fs.getFileStatus(new Path(rightDir));
			if (!rightDirStatus.isDir())
			{
				System.out.println("right file length: " + rightDirStatus.getLen());
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
			e.printStackTrace();
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

		for (DoubleDryRun sampler : instances)
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

		System.out.println("total sample times: " + count + "\tskipped: " + skipped);
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
			long leftWeight = (long) Math.pow(2, left.getValue().size());
			System.out.println("left record length: " + left.getValue().size() + "\t" + left.getKey());

			for (Map.Entry<Path, Long> rp : rightPaths.entrySet()) // iterate all the right files
			{
				// open this file
				for (long i = 0; i < rp.getValue(); i++)
				{
					long offset = rightLineLength * i;
					for (DoubleDryRun sampler : instances)
					{
						count++;
						reporter.incrCounter("dps.record.sample", "tuple", 1);
						if (sampler.sampleDryRun(leftWeight))
						{
							reporter.incrCounter("dps.record.sample", "succ-dry", 1);
							// read, then sample
							Set<String> rightRecord = readRecordAsSet(fs, rp.getKey(), offset);
							//							Set<String> rightRecord = new HashSet<String>(Arrays.asList("1", "2", "3", "4", "5"));
							int intersect = Sets.intersection(left.getValue(), rightRecord).size();
							long weight = leftWeight - (long)Math.pow(2, intersect);
							
							//String rightIndex = rp.getKey().toString() + Config.SepFilePosition + offset;
							//String combinedIndex = left.getKey() + Config.SepIndexes + rightIndex;

							if (sampler.sample(weight, " "))
							{
								succ++;
								System.out.println("sampled right line: " + i + "/" + rp.getValue());
							}
						}
						else
						{
							skipped++;
						}
					}

					if (i % 10000 == 0)
					{
						reporter.progress();
					}

				}
			}
		}
		return true;
	}


	private Set<String> readRecordAsSet(FileSystem fs, Path path, long offset) throws IOException
	{
		FSDataInputStream fsInputStream = fs.open(path);
		fsInputStream.seek(offset);
		BufferedReader reader = new BufferedReader(new InputStreamReader(fsInputStream));
		String[] line = reader.readLine().trim().split(Config.SepItemsRegex);
		reader.close();
		return Sets.newHashSet(line);
		//		return new HashSet<String>(Arrays.asList(line));
	}


	private String calcWeight(Set<String> left, Set<String> right)
	{
		int intersect = Sets.intersection(left, right).size();
		int difference = Sets.difference(left, right).size();

		BigInteger firstPart = new BigInteger("2").pow(difference).subtract(BigInteger.ONE);
		BigInteger secondPart = new BigInteger("2").pow(intersect);
		return firstPart.multiply(secondPart).toString();
	}
}
