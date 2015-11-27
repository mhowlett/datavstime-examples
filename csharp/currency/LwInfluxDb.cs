using System;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.IO;
using System.Linq;
using Newtonsoft.Json;

namespace LwInfluxDb
{
    /// <summary>
    ///   The obvious implementation of ISeriesPoint.
    ///   You may wish to write a more constrained version to more conveniently match your requirements.
    /// </summary>
    public class SeriesPoint : ISeriesPoint
    {	
	    public string Name { get; set; }
	
    	public Dictionary<string, string> Tags { get; set; }
	
	    public List<string> Fields { get; set; }
	
    	public List<object> Values { get; set; }

        public DateTime? Timestamp { get; set; }
    }
    
    public interface ISeriesPoint
    {
	    string Name { get; }
	
	    Dictionary<string, string> Tags { get; }
	
	    List<string> Fields { get; }
	
	    List<object> Values { get; }

        DateTime? Timestamp { get; }
    }

    public class InfluxDb
    {
        private string _url;
        private string _credentials;

        public InfluxDb(string url, string db)
        {
            _url = url;
            if (!_url.EndsWith("/")) { _url = _url + "/"; }
            //_credentials = "?db=" + db + "&u=" + user + "&p=" + password;
            _credentials = "?db=" + db;
            Timeout = TimeSpan.FromSeconds(15);
        }

        public TimeSpan Timeout { get; set; }

        private static string SerializeWriteData(ISeriesPoint data)
        {
            if (data.Fields.Count != data.Values.Count)
            {
                throw new Exception("Invalid field series point - number of fields does not match number of values.");
            }
            var result = data.Name.Replace(",", "\\,").Replace(" ", "\\ ");
            if (data.Tags != null && data.Tags.Count > 0)
            {
                foreach (var kvp in data.Tags)
                {
                    result += "," + kvp.Key + "=" + kvp.Value;
                }
            }
            result += " ";
            var separatorNeeded = false;
            for (int i = 0; i < data.Fields.Count; ++i)
            {
                string strRep;
                var value = data.Values[i];
                if (value == null)
                {
                    continue;
                }
                else if (value is double)
                {
                    strRep = ((double)value).ToString(".0##############");
                }
                else if (value is float)
                {
                    strRep = ((float)value).ToString(".0######");
                }
                else if (value is decimal || value is Int16 || value is Int32 || value is Int64 || value is UInt16 || value is UInt32 || value is UInt64)
                {
                    strRep = string.Format("{0}", value);
                }
                else if (value is bool)
                {
                    strRep = ((bool)value) ? "t" : "f";
                }
                else
                {
                    strRep = string.Format("\"{0}\"", string.Format("{0}", value).Replace("\"", "\\\""));
                }

                if (separatorNeeded)
                {
                    result += ",";
                }
                else
                {
                    separatorNeeded = true;
                }

                result += data.Fields[i] + "=" + strRep;
            }
            if (data.Timestamp.HasValue)
            {
                result += string.Format(" {0}", ToUnixTimeNanoseconds(data.Timestamp.Value));
            }
            return result;
        }

        public static long ToUnixTimeNanoseconds(DateTime date)
        {
            return (date.ToUniversalTime().Ticks - 621355968000000000) * 100;
        }

        public async Task WriteAsync(List<ISeriesPoint> data)
        {
            string s = "";
            foreach (var d in data)
            {
                s += SerializeWriteData(d) + "\n";
            }
            Console.WriteLine(s);
            await WriteAsync(s);
        }
        
        public async Task WriteAsync(ISeriesPoint data)
        {
            await WriteAsync(SerializeWriteData(data));
        }

        private async Task WriteAsync(string data)
        {
            using (var client = new HttpClient())
            {
                client.Timeout = Timeout;
                client.BaseAddress = new Uri(_url);
                client.DefaultRequestHeaders.Accept.Clear();
                HttpResponseMessage response = await client.PostAsync("write" + _credentials, new StringContent(data));
                if (!response.IsSuccessStatusCode)
                {
                    throw new Exception(
                        string.Format("Unable to write series point data. status: {0}, reason: {1}, result: {2}",
                            response.StatusCode, response.ReasonPhrase, await response.Content.ReadAsStringAsync()));
                }
            }
        }

        public class QuerySeries
        {
            public string Name;
            public List<string> Columns;
            public List<List<object>> Values;
        }

        public async Task<List<List<object>>> QuerySingleSeriesAsync(string query)
        {
            var queryString = Uri.EscapeDataString(query);
            using (var client = new HttpClient())
            {
                client.Timeout = Timeout;
                client.BaseAddress = new Uri(_url);
                client.DefaultRequestHeaders.Accept.Clear();
                client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
                var url = "query" + _credentials + "&q=" + queryString;
                HttpResponseMessage response = await client.GetAsync(url);
                if (!response.IsSuccessStatusCode)
                {
                    throw new Exception("unable to read series point data.");
                }
                using (var stream = await response.Content.ReadAsStreamAsync())
                using (var sr = new StreamReader(stream))
                {
                    var result = JsonConvert.DeserializeObject<Dictionary<string, List<Dictionary<string, List<QuerySeries>>>>>(sr.ReadToEnd());
                    if (result.ContainsKey("results"))
                    {
                        if (result["results"].Count == 1)
                        {
                            if (result["results"][0].ContainsKey("series"))
                            {
                                if (result["results"][0]["series"].Count == 1)
                                {
                                    return result["results"][0]["series"][0].Values;
                                }
                            }
                        }
                    }
                    return null;
                }
            }
        }

        public async Task<List<List<List<object>>>> QueryMultipleSeriesAsync(string query)
        {
            var queryString = Uri.EscapeDataString(query);
            using (var client = new HttpClient())
            {
                client.Timeout = Timeout;
                client.BaseAddress = new Uri(_url);
                client.DefaultRequestHeaders.Accept.Clear();
                client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
                var url = "query" + _credentials + "&q=" + queryString;
                HttpResponseMessage response = await client.GetAsync(url);
                if (!response.IsSuccessStatusCode)
                {
                    throw new Exception("unable to read series point data.");
                }
                using (var stream = await response.Content.ReadAsStreamAsync())
                using (var sr = new StreamReader(stream))
                {
                    var result = JsonConvert.DeserializeObject<Dictionary<string, List<Dictionary<string, List<QuerySeries>>>>>(sr.ReadToEnd());
                    return result["results"][0]["series"].Select(a => a.Values).ToList();
                }
            }
        }
        
    }

}
