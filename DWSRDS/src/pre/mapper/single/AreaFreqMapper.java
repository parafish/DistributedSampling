package pre.mapper.single;

import java.math.BigInteger;

public class AreaFreqMapper extends AbstractSingleMapper
{
	@Override
	protected <T> BigInteger calcWeight(T[] items)
	{
		int length = items.length;// > 160  ? 160 : items.length;
		return new BigInteger("2").pow(length-1).multiply(new BigInteger(String.valueOf(length)));
	}

}
