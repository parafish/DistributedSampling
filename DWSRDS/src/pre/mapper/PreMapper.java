package pre.mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Define the format of the mappers in the processing (weighting) step.
 * Output: <Text("1"), index-weight pair>
 * @author zheyi
 *
 */
public class PreMapper extends Mapper<LongWritable, Text, Text, Text>
{
	// nothing 
}
