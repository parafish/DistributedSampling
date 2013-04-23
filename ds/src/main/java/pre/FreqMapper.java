package pre;


/**
 * Maps the offset/text pairs to offset/weight pairs, with the help of its superclass.<p>
 * The weight are caculated as follows:
 * <blockquote>
 * w = 2^|R|
 * </blockquote>
 * where |R| is the length of the record
 * @author zheyi
 *
 */
public class FreqMapper extends AbstractSingleMapper
{
	
	@Override
	protected <T> long calcWeight(T[] items)
	{
		int  exp = items.length > maxRecordLength? maxRecordLength : items.length;
		return (long)Math.pow(2, exp);
	}
}
