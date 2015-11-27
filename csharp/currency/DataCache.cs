using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using LwInfluxDb;

namespace DataVsTime
{
    public class DataCache
    {
        private readonly InfluxDb _influxDb;

        private object _valuesLock = new object();
        private Dictionary<int, OneDaysData> _values = new Dictionary<int, OneDaysData>();
        
        public List<string> Currencies { get; set; }
        public List<CurrencyPair> CurrencyPairs { get; set; }

        readonly DateTime _unixZero = new DateTime(1970, 1, 1);
        readonly DateTime _baseDate = new DateTime(2010, 1, 1);

        private int calculateDayNumber(DateTime dt) { return (int)Math.Floor((dt - _baseDate).TotalDays); }
        private DateTime calculateDayStart(int dayNumber) { return _baseDate + TimeSpan.FromDays(dayNumber); }
        private DateTime calculateDayEnd(int dayNumber) { return _baseDate + TimeSpan.FromDays(dayNumber + 1); }

        private void retrieveSymbolLists()
        {
            var r = _influxDb.QueryMultipleSeriesAsync("SHOW SERIES");
            r.Wait();
            Currencies = r.Result.Select(v => (string)v[0][0]).ToList();
            Currencies.Add("USD");
            CurrencyPairs = new List<CurrencyPair>();

            for (var i = 0; i < Currencies.Count; ++i)
            {
                for (var j = 0; j < Currencies.Count; ++j)
                {
                    if (i == j) { continue; }
                    var cp = new CurrencyPair
                    {
                        Currency = Currencies[i],
                        CurrencyRelativeTo = Currencies[j]
                    };
                    CurrencyPairs.Add(cp);
                }
            }
        }

        private void sampleRawDataThenAddToValues(Dictionary<string, object> rawData, int dayNumber, int binSize_seconds)
        {
            var dfd = new OneDaysData();
            dfd.BinSize = binSize_seconds;

            foreach (var kvp in rawData)
            {
                var symbol = kvp.Key;
                var dataRaw = (List<List<object>>)kvp.Value;

                // parse into something more manageable.
                var dates = new List<DateTime>();
                var values = new List<double>();
                for (int i = 0; i < dataRaw.Count; ++i)
                {
                    DateTime dateVal;
                    if (dataRaw[i][0] is string)
                    {
                        var dateStr = (string) dataRaw[i][0];
                        dateVal = DateTime.Parse(dateStr, null, System.Globalization.DateTimeStyles.RoundtripKind);
                    }
                    else if (dataRaw[i][0] is DateTime)
                    {
                        dateVal = (DateTime) dataRaw[i][0];
                    }
                    else
                    {
                        continue;
                    }

                    var value = (double)dataRaw[i][1];
                    dates.Add(dateVal);
                    values.Add(value);
                }

                // interporlate in to equi-spaced bins.
                int currentRawIndex = 0;
                var sampled = new List<double>();
                var dayStart = this.calculateDayStart(dayNumber);
                for (int cTime = 0; cTime<60*60*24; cTime += binSize_seconds)
                {
                    while (   currentRawIndex + 1 < dates.Count
                           && dates[currentRawIndex + 1] < dayStart + TimeSpan.FromSeconds(cTime))
                    {
                        currentRawIndex += 1;
                    }

                    if (dates[currentRawIndex] > dayStart + TimeSpan.FromSeconds(cTime))
                    {
                        sampled.Add(_values.ContainsKey(dayNumber - 1)
                            ? _values[dayNumber - 1].ValueAtTime(symbol, 60*60*24 - 1)
                            : double.NaN);
                    }
                    else if (dates[dates.Count-1] + TimeSpan.FromSeconds(binSize_seconds) < dayStart + TimeSpan.FromSeconds(cTime))
                    {
                        sampled.Add(double.NaN);
                    }
                    else
                    {
                        sampled.Add(values[currentRawIndex]);
                    }
                }

                dfd.Values.Add(symbol, sampled.ToArray());
            }

            if (_values.ContainsKey(dayNumber))
            {
                Console.WriteLine("replaceing data for day #: " + dayNumber);
                _values[dayNumber] = dfd;
            }
            else
            {
                Console.WriteLine("adding data for day #: " + dayNumber);
                _values.Add(dayNumber, dfd);
            }
        }

        private Dictionary<string, object> getRawDataForDay(int dayNumber)
        {
            var start_dt = calculateDayStart(dayNumber);
            var stop_dt = calculateDayEnd(dayNumber);

            var result = new Dictionary<string, object>();
            foreach (var currency in Currencies)
            {
                if (currency == "USD")
                {
                    continue;
                }

                var query = "SELECT time, value FROM " + currency + " WHERE time >= '"
                            + start_dt.ToString("yyyy-MM-ddTHH:mm:ss.fffffZ")
                            + "' AND time <= '"
                            + stop_dt.ToString("yyyy-MM-ddTHH:mm:ss.fffffZ")
                            + "'";
                try
                {
                    var r = _influxDb.QuerySingleSeriesAsync(query);
                    r.Wait();
                    if (r.Result != null)
                    {
                        result.Add(currency, r.Result);
                    }
                    else
                    {
                        return null;
                    }
                }
                catch (Exception)
                {
                    Console.WriteLine("ERROR: InfluxDb query execution failed.ge");
                    return null;
                }
            }
            return result;
        }

        public DataCache(string influxInstance)
        {
            _influxDb = new InfluxDb(influxInstance, "currency");
            retrieveSymbolLists();
            new Thread(() => {
                while (true)
                {
                    int currentDay = calculateDayNumber(DateTime.UtcNow);

                    lock (_valuesLock)
                    {
                        // 1. remove any data for days that are now out of range.
                        var toDelete = new List<int>();
                        foreach (var kvp in _values)
                        {
                            if (kvp.Key < currentDay - 7)
                            {
                                toDelete.Add(kvp.Key);
                            }
                        }
                        foreach (var key in toDelete)
                        {
                            _values.Remove(key);
                        }
                    }

                    // 2. add day data where required and we don't have, or it's today.
                    // TODO: something more efficient for today.
                    for (int i = 7; i >= 0; --i)
                    {
                        bool haveDay;
                        lock (_valuesLock)
                        {
                            haveDay = _values.ContainsKey(currentDay - i) && i != 0;
                        }
                        if (!haveDay)
                        {
                            var rawData = getRawDataForDay(currentDay - i);
                            if (rawData != null)
                            {
                                lock (_valuesLock)
                                {
                                    sampleRawDataThenAddToValues(rawData, currentDay - i, 30);
                                }
                            }
                            else
                            {
                                Console.WriteLine("no data for for day #: " + (currentDay - i));
                            }
                        }
                    }

                    Thread.Sleep(TimeSpan.FromSeconds(20));
                }
            }).Start();
        }

        public double[] GetCurrencyData(string symbol, long start_ms, long stop_ms, long step_ms)
        {
            var resultLength = (stop_ms - start_ms)/step_ms;
            if ((stop_ms - start_ms)%step_ms != 0)
            {
                resultLength += 1;
            }
            var result = new double[resultLength];

            lock (_valuesLock)
            {
                for (int i = 0; i < resultLength; ++i)
                {
                    var current_dt = _unixZero + TimeSpan.FromMilliseconds(start_ms + step_ms*i);
                    var dn = calculateDayNumber(current_dt);
                    if (!_values.ContainsKey(dn))
                    {
                        result[i] = double.NaN;
                        continue;
                    }

                    var current_offset = new TimeSpan(0, current_dt.Hour, current_dt.Minute, current_dt.Second, current_dt.Millisecond);
                    result[i] = _values[dn].ValueAtTime(symbol, (int)current_offset.TotalSeconds);
                }
            }

            return result;
        }

    }
}
