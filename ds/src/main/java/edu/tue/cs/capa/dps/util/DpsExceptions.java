package edu.tue.cs.capa.dps.util;

public class DpsExceptions
{
	private DpsExceptions()
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

		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;


		public NonFixedLineLengthException()
		{
			super();
		}


		public NonFixedLineLengthException(String s)
		{
			super(s);
		}


		public NonFixedLineLengthException(Throwable t)
		{
			super(t);
		}


		public NonFixedLineLengthException(String s, Throwable t)
		{
			super(s, t);
		}
	}

}
