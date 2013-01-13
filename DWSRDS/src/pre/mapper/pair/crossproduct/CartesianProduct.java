package pre.mapper.pair.crossproduct;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.join.CompositeInputSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;

import setting.PARAMETERS;

public class CartesianProduct
{
	public static class CartesianInputFormat extends InputFormat<Text, Text>
	{
		public static final String LEFT_INPUT_FORMAT = "cart.left.inputformat";
		public static final String LEFT_INPUT_PATH = "cart.left.path";

		public static final String RIGHT_INPUT_FORMAT = "cart.right.inputformat";
		public static final String RIGHT_INPUT_PATH = "cart.right.path";


		public static void setLeftInputInfo(Job job, Class<? extends FileInputFormat> inputFormat,
						String inputPath)
		{
			job.getConfiguration().set(LEFT_INPUT_FORMAT, inputFormat.getCanonicalName());
			job.getConfiguration().set(LEFT_INPUT_PATH, inputPath);
		}


		public static void setRightInputInfo(Job job, Class<? extends FileInputFormat> inputFormat,
						String inputPath)
		{
			job.getConfiguration().set(RIGHT_INPUT_FORMAT, inputFormat.getCanonicalName());
			job.getConfiguration().set(RIGHT_INPUT_PATH, inputPath);
		}


		public List<InputSplit> getInputSplits(JobContext jobContext, String inputFormatClass,
						String inputPath) throws IOException
		{
			// create a new instance of the input format
			FileInputFormat inputFormat;
			try
			{
				inputFormat = (FileInputFormat) ReflectionUtils.newInstance(
								Class.forName(inputFormatClass), jobContext.getConfiguration());

				// set the input path for the left data set
				// XXX: dummy job
				Job job = new Job();
				inputFormat.setInputPaths(job, inputPath);

				// get the left input splits
				return inputFormat.getSplits(job);
			}
			catch (ClassNotFoundException e)
			{
				e.printStackTrace();
			}

			return new ArrayList<InputSplit>();

		}


		@Override
		public List<InputSplit> getSplits(JobContext jobContext) throws IOException
		{
			// get the input splits from both the left and right data sets
			List<InputSplit> leftSplits = getInputSplits(jobContext, jobContext.getConfiguration()
							.get(LEFT_INPUT_FORMAT),
							jobContext.getConfiguration().get(LEFT_INPUT_PATH));
			List<InputSplit> rightSplits = getInputSplits(jobContext, jobContext.getConfiguration()
							.get(RIGHT_INPUT_FORMAT),
							jobContext.getConfiguration().get(RIGHT_INPUT_PATH));

			// create our CompositeInputSplits, left.length * right.length
			List<InputSplit> returnSplits = new ArrayList<InputSplit>(leftSplits.size()
							* rightSplits.size());
			try
			{
				for (InputSplit left : leftSplits)
					for (InputSplit right : rightSplits)
					{
						CompositeInputSplit oneSplit = new CompositeInputSplit(2);
						oneSplit.add(left);
						oneSplit.add(right);

						returnSplits.add(oneSplit);
					}
			}
			catch (InterruptedException e)
			{
				e.printStackTrace();
			}

			System.out.println("# splits : " + returnSplits.size());

			return returnSplits;
		}


		@Override
		public RecordReader<Text, Text> createRecordReader(InputSplit split,
						TaskAttemptContext context) throws IOException, InterruptedException
		{
			// create a new instance of the Cartesian record reader
			return new CartesianRecordReader();
		}

	}

	public static class CartesianRecordReader extends RecordReader<Text, Text>
	{
		// record readers to get key value pairs
		private RecordReader leftRR = null;
		private RecordReader rightRR = null;

		// store configuration to re-create the right record reader
		private FileInputFormat rightFIF;
		private TaskAttemptContext rightContext;
		private InputSplit rightIS;

		// helpers
		private boolean goToNextLeft = true;
		private boolean alldone = false;

		// key, values
		private Object lkey;
		private Text lvalue;
		private Object rkey;
		private Text rvalue;


		@Override
		public void initialize(InputSplit _split, TaskAttemptContext context) throws IOException,
						InterruptedException
		{
			CompositeInputSplit split = (CompositeInputSplit) _split;

			this.rightContext = context;
			this.rightIS = split.get(1);

			try
			{
				// create left record reader
				FileInputFormat leftFIF = (FileInputFormat) ReflectionUtils.newInstance(
								Class.forName(context.getConfiguration().get(
												CartesianInputFormat.LEFT_INPUT_FORMAT)),
								context.getConfiguration());

				leftRR = leftFIF.createRecordReader(split.get(0), context);
				leftRR.initialize(split.get(0), context);

				// create right record reader
				rightFIF = (FileInputFormat) ReflectionUtils.newInstance(
								Class.forName(context.getConfiguration().get(
												CartesianInputFormat.RIGHT_INPUT_FORMAT)),
								context.getConfiguration());
				rightRR = rightFIF.createRecordReader(rightIS, context);
				rightRR.initialize(rightIS, context);
			}
			catch (ClassNotFoundException e)
			{
				e.printStackTrace();
			}
		}


		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException
		{
			do
			{
				// read the next left record?
				if (goToNextLeft)
				{
					if (!leftRR.nextKeyValue())
					{
						alldone = true;
						break;
					}
					else
					{
						lkey = leftRR.getCurrentKey();
						lvalue = (Text) leftRR.getCurrentValue();

						goToNextLeft = false;
						alldone = false;

						rightRR = rightFIF.createRecordReader(rightIS, rightContext);
						rightRR.initialize(rightIS, rightContext);
					}
				}

				// read the next key value pair from the right data set
				if (rightRR.nextKeyValue())
				{
					rkey = rightRR.getCurrentKey();
					rvalue = (Text) rightRR.getCurrentValue();
				}
				else
				{
					goToNextLeft = true;
					rightRR.close();
				}
			} while (goToNextLeft);

			return !alldone;
		}


		@Override
		public Text getCurrentKey() throws IOException, InterruptedException
		{
			return new Text(lkey.toString() + PARAMETERS.SepIndexes + rkey.toString());
		}


		@Override
		public Text getCurrentValue() throws IOException, InterruptedException
		{
			return new Text(lvalue.toString() + PARAMETERS.SepRecords + rvalue.toString());
		}


		@Override
		public float getProgress() throws IOException, InterruptedException
		{
			return leftRR.getProgress();
		}


		@Override
		public void close() throws IOException
		{
			leftRR.close();
			rightRR.close();
		}

	}

	// complete, for testing only
	public static class CartesianMapper extends Mapper<Text, Text, Text, Text>
	{
		@Override
		public void map(Text key, Text value, Context context) throws IOException,
						InterruptedException
		{
			context.write(key, value);
		}


		@Override
		public void cleanup(Context context) throws IOException, InterruptedException
		{
//			context.write(NullWritable.get(), new Text(String.valueOf(count)));
		}
	}

	// complete
	public static void main(String[] args) throws IOException, InterruptedException,
					ClassNotFoundException
	{

		Path inputLeft = new Path("/home/zheyi/sampling/data/test.dat");
		Path inputRight = inputLeft;
		Path output = new Path("/home/zheyi/sampling/output");

		Job job = new Job();
		job.setJarByClass(CartesianProduct.class);

		job.setMapperClass(CartesianMapper.class);
		job.setNumReduceTasks(0);

		job.setInputFormatClass(CartesianInputFormat.class);

		// configure the input format
		CartesianInputFormat.setLeftInputInfo(job, TextInputFormat.class, inputLeft.toString());
		CartesianInputFormat.setRightInputInfo(job, TextInputFormat.class, inputRight.toString());

		FileOutputFormat.setOutputPath(job, output);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileSystem fs = FileSystem.get(job.getConfiguration());
		fs.delete(output, true);

		int exitCode = job.waitForCompletion(true) ? 0 : 1;

		System.exit(exitCode);

	}

}
