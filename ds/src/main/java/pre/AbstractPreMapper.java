package pre;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;

/**
 * Fixes the input and output format for the preprocessing step. <p>
 * The input pairs are in the format <code>Writable</code>/<code>Text</code>;
 * the output pairs are in the format <code>Writable</code>/<code>Text</code>
 * @see AbstractSingleMapper
 * @see AbstractPairMapper
 * @author zheyi
 *
 */
public abstract class AbstractPreMapper extends MapReduceBase 
			implements Mapper<Writable, Text, Writable, Text>
{
	
}
