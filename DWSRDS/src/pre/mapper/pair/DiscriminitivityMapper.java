package pre.mapper.pair;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class DiscriminitivityMapper extends AbstractPairMapper
{

	@Override
	protected <T> BigInteger calcWeight(T [] positive, T [] negative)
	{
		Set<T> negativeSet = new HashSet<T>(Arrays.asList(negative));
		
		Set<T> intersect = new HashSet<T> (Arrays.asList(positive));
		intersect.retainAll(negativeSet);
		
		Set<T> complement = new HashSet<T>(Arrays.asList(positive));
		complement.removeAll(negativeSet);
		
		BigInteger firstPart = new BigInteger("2").pow(complement.size()).subtract(BigInteger.ONE);
		BigInteger secondPart = new BigInteger("2").pow(intersect.size());
		return firstPart.multiply(secondPart);
	}

}
