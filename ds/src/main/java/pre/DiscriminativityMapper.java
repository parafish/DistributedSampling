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
public class DiscriminativityMapper extends AbstractPairMapper
{

	@Override
	protected <T> BigInteger calcWeight(T [] positive, T [] negative)
	{
		Set<T> negativeSet = new HashSet<T>(Arrays.asList(negative));
		Set<T> intersect = new HashSet<T> (Arrays.asList(positive));
		intersect.retainAll(negativeSet);
		
		Set<T> complement = new HashSet<T>(Arrays.asList(positive));
		complement.removeAll(negativeSet);
		
		// XXX: not that precise
		int complementSize = complement.size() > maxRecordLength ? maxRecordLength : complement.size();
		int intersectSize = intersect.size() > maxRecordLength ? maxRecordLength : intersect.size();
		
		BigInteger firstPart = new BigInteger("2").pow(complementSize).subtract(BigInteger.ONE);
		BigInteger secondPart = new BigInteger("2").pow(intersectSize);
		return firstPart.multiply(secondPart);
	}

}
