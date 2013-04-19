package pre;

import java.io.IOException;
import java.util.logging.Logger;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.join.CompositeInputSplit;
import org.apache.hadoop.util.ReflectionUtils;

import util.Config;


/**
 * Includes three public static classes, together providing a function that individually reads
 * one line from two input files, and outputs to a mapper with the format: 
 * <code>index1,index2 text1,text2</code>.
 * @author zheyi
 *
 */
public class CartesianProduct
{
	private final static Logger	LOGGER	= Logger.getLogger(CartesianProduct.class.getName());

	public static class CartesianInputFormat extends FileInputFormat
	{
		public static final String	LEFT_INPUT_FORMAT	= "cart.left.inputformat";
		public static final String	LEFT_INPUT_PATH		= "cart.left.path";

		public static final String	RIGHT_INPUT_FORMAT	= "cart.right.inputformat";
		public static final String	RIGHT_INPUT_PATH	= "cart.right.path";


		public static void setLeftInputInfo(JobConf job, Class<? extends FileInputFormat> inputFormat, String inputPath)
		{
			job.set(LEFT_INPUT_FORMAT, inputFormat.getCanonicalName());
			job.set(LEFT_INPUT_PATH, inputPath);
		}


		public static void setRightInputInfo(JobConf job, Class<? extends FileInputFormat> inputFormat, String inputPath)
		{
			job.set(RIGHT_INPUT_FORMAT, inputFormat.getCanonicalName());
			job.set(RIGHT_INPUT_PATH, inputPath);
		}


		private InputSplit[] getInputSplits(JobConf conf, String inputFormatClass, String inputPath, int numSplits)
						throws IOException
		{
			try
			{
				FileInputFormat inputFormat = (FileInputFormat) ReflectionUtils.newInstance(
								Class.forName(inputFormatClass), conf);
				FileInputFormat.setInputPaths(conf, inputPath);
				return inputFormat.getSplits(conf, numSplits);
			}
			catch (ClassNotFoundException e)
			{
				e.printStackTrace();
			}

			return null;
		}


		public InputSplit[] getSplits(JobConf conf, int numSplits) throws IOException
		{
			InputSplit[] leftSplits = getInputSplits(conf, conf.get(LEFT_INPUT_FORMAT), conf.get(LEFT_INPUT_PATH),
							numSplits);
			InputSplit[] rightSplits = getInputSplits(conf, conf.get(RIGHT_INPUT_FORMAT), conf.get(RIGHT_INPUT_PATH),
							numSplits);

			CompositeInputSplit[] returnSplits = new CompositeInputSplit[leftSplits.length * rightSplits.length];

			int i = 0;
			for (InputSplit left : leftSplits)
				for (InputSplit right : rightSplits)
				{
					returnSplits[i] = new CompositeInputSplit(2);
					returnSplits[i].add(left);
					returnSplits[i].add(right);
					i++;
				}
			LOGGER.info("# splits : " + returnSplits.length);
			return returnSplits;
		}


		@Override
		public RecordReader getRecordReader(InputSplit split, JobConf job, Reporter reporter) throws IOException
		{
			return new CartesianRecordReader((CompositeInputSplit) split, job, reporter);
		}
	}


	public static class CartesianRecordReader<K1, V1, K2, V2> implements RecordReader<Text, Text>
	{
		// record readers to get key value pairs
		private RecordReader	leftRR			= null;
		private RecordReader	rightRR			= null;

		// store configuration to re-create the right record reader
		private FileInputFormat	rightFIF;
		private JobConf			rightConf;
		private InputSplit		rightIS;
		private Reporter		rightReporter;

		// helpers
		private boolean			goToNextLeft	= true;
		private boolean			alldone			= false;

		// key, values
		private K1				lkey;
		private V1				lvalue;
		private K2				rkey;
		private V2				rvalue;


		public CartesianRecordReader(CompositeInputSplit split, JobConf conf, Reporter reporter) throws IOException
		{
			this.rightConf = conf;
			this.rightIS = split.get(1);
			this.rightReporter = reporter;

			// create left record reader
			FileInputFormat leftFIF = null;
			try
			{
				leftFIF = (FileInputFormat) ReflectionUtils.newInstance(
								Class.forName(conf.get(CartesianInputFormat.LEFT_INPUT_FORMAT)), conf);
			}
			catch (ClassNotFoundException e)
			{
				e.printStackTrace();
			}
			leftRR = leftFIF.getRecordReader(split.get(0), conf, reporter);

			// create right record reader
			try
			{
				rightFIF = (FileInputFormat) ReflectionUtils.newInstance(
								Class.forName(conf.get(CartesianInputFormat.RIGHT_INPUT_FORMAT)), conf);
			}
			catch (ClassNotFoundException e)
			{
				e.printStackTrace();
			}
			rightRR = rightFIF.getRecordReader(rightIS, conf, reporter);

			lkey = (K1) this.leftRR.createKey();
			lvalue = (V1) this.leftRR.createValue();
			rkey = (K2) this.rightRR.createKey();
			rvalue = (V2) this.rightRR.createValue();
		}


		public boolean next(Text key, Text value) throws IOException
		{
			do
			{

				// if we are to go to the next left key/value pair
				if (goToNextLeft)
				{
					if (!leftRR.next(lkey, lvalue))
					{
						alldone = true;
						break;
					}
					else
					{
						goToNextLeft = false;
						alldone = false;

						this.rightRR = this.rightFIF.getRecordReader(this.rightIS, this.rightConf, this.rightReporter);

					}
				}

				if (rightRR.next(rkey, rvalue))
				{
					// return key/value
					key.set(lkey.toString() + Config.SepIndexes + rkey.toString());
					value.set(lvalue.toString() + Config.SepRecords + rvalue.toString());
				}
				else
				{
					goToNextLeft = true;
					rightRR.close();
				}
			} while (goToNextLeft);

			return !alldone;
		}


		public Text createKey()
		{
			return new Text();
		}


		public Text createValue()
		{
			return new Text();
		}


		public long getPos() throws IOException
		{
			return leftRR.getPos();
		}


		public void close() throws IOException
		{
			leftRR.close();
			rightRR.close();
		}


		public float getProgress() throws IOException
		{
			return leftRR.getProgress();
		}
	}

}
