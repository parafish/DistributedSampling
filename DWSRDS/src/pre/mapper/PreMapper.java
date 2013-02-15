package pre.mapper;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Define the format of the mappers in the processing (weighting) step.
 * Output: <index-pair, record-pair>
 * @author zheyi
 *
 */
public class PreMapper extends Mapper<Object, Text, NullWritable, Text>
{
//	protected int maxlength;
//	
//	@Override
//	public void setup(Context context)
//	{
//		maxlength = context.getConfiguration().getInt(PARAMETERS.MAX_LENGTH, 100);
//	}
}
