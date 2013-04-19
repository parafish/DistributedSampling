package discriminativity;

public class DspExceptions
{
	private DspExceptions()
	{
	};


	// if required parameters not set
	public static class MissingParameterException extends RuntimeException
	{
		private static final long serialVersionUID = -2417621044827629526L;


		public MissingParameterException()
		{
			super();
		}


		public MissingParameterException(String s)
		{
			super(s);
		}


		public MissingParameterException(Throwable t)
		{
			super(t);
		}


		public MissingParameterException(String s, Throwable t)
		{
			super(s, t);
		}
	}
	
	public static class NonFixedLineLengthException extends RuntimeException
	{
		
	}

}
