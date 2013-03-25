package pre;

import static util.Parameters.DEBUG_MODE;

import java.util.logging.Logger;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;

import sample.record.RecordSamplingMapper;
import util.Parameters;


/**
 * Fixes the input and output format for the preprocessing step.
 * <p>
 * The input pairs are in the format <code>Writable</code>/<code>Text</code>;
 * the output pairs are in the format <code>Writable</code>/<code>Text</code>
 * 
 * @see AbstractSingleMapper
 * @see AbstractPairMapper
 * @author zheyi
 * 
 */
public abstract class AbstractPreMapper extends MapReduceBase implements Mapper<Writable, Text, Writable, Text>
{
	private final static Logger	LOGGER			= Logger.getLogger(RecordSamplingMapper.class.getName());

	protected int				maxRecordLength	= Parameters.DEFAULT_RECORD_LENGTH;


	@Override
	public void configure(JobConf jobConf)
	{
		maxRecordLength = jobConf.getInt(Parameters.MAX_RECORD_LENGTH, Parameters.DEFAULT_RECORD_LENGTH);
		if (DEBUG_MODE)
			LOGGER.info("Max record length: " + maxRecordLength);
	}
}
