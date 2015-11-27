using System;
using System.Collections.Generic;

namespace DataVsTime
{
    public class OneDaysData
    {
        public int BinSize;

        public Dictionary<string, double[]> Values = new Dictionary<string, double[]>();

        public double ValueAtTime(string symbol, int seconds)
        {
            try
            {
                return Values[symbol][seconds/BinSize];
            }
            catch (Exception)
            {
                Console.WriteLine(
                    "ERROR: ValueAtTime not available at bin: " + seconds/BinSize + 
                    " seconds: " + seconds + 
                    " values length: " + Values[symbol].Length);
                return double.NaN;
            }
        }
    }
}
