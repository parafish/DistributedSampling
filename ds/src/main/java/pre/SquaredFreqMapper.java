package pre;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * Maps a pair of records to a weight, which is calculated as: 
 * <blockquote>
 * w = (see the paper)
 * </blockquote>
 * @author zheyi
 *
 */
public class SquaredFreqMapper extends AbstractPairMapper
{
	@Override
	protected <T> BigInteger calcWeight(T [] items1, T [] items2)
	{
		Set<T> intersect = new HashSet<T>(Arrays.asList(items1));
		intersect.retainAll(new HashSet<T>(Arrays.asList(items2)));
				
		int intersectSize = intersect.size() > maxRecordLength ? maxRecordLength : intersect.size();
		return new BigInteger("2").pow(intersectSize);
	}

}
