using System;
using System.IO;
using System.Collections.Generic;
using System.Linq;
using Microsoft.AspNet.Builder;
using Microsoft.AspNet.Hosting;
using Microsoft.AspNet.Http;
using Newtonsoft.Json;


// Note on not available data:
//   in returned data "NaN" or 0/0 will work.

namespace DataVsTime
{
    public class Startup
    {
        private DataCache _dataCache;

        public Startup()
        {
            var influxInstance = Environment.GetEnvironmentVariable("INFLUX_CURRENCYPROXY");
            _dataCache = new DataCache(influxInstance);
        }

        private Dictionary<string,string> parseSeriesSet(string s)
        {
            var result = new Dictionary<string,string>();
            if (!s.StartsWith("{") || !s.EndsWith("}"))
            {
                return result;
            }
            s = s.Substring(1, s.Length-2);
            var parts = s.Split(',');
            foreach (var part in parts)
            {
                var kv = part.Split(':');
                if (kv.Length == 2 && kv[1].StartsWith("'") && kv[1].EndsWith("'"))
                {
                    result.Add(kv[0],kv[1].Substring(1, kv[1].Length-2));
                }
            }
            return result;
        }

        private void HandleDataRequest(StreamWriter sw, Dictionary<string, string> seriesSetSpec, long start, long stop, long step)
        {
            if (!seriesSetSpec.ContainsKey("currency")) { return; }
            if (!seriesSetSpec.ContainsKey("currency_relative_to")) { return; }

            var currency = seriesSetSpec["currency"];
            var currencyRelativeTo = seriesSetSpec["currency_relative_to"];

            double[] currencyData = null;
            double[] currencyRelativeToData = null;

            if (currency != "USD")
            {
                // possibly wasteful.
                currencyData = _dataCache.GetCurrencyData(currency, start, stop, step);
            }
            if (currencyRelativeTo != "USD")
            {
                currencyRelativeToData = _dataCache.GetCurrencyData(currencyRelativeTo, start, stop, step);
            }

            double[] data = null;
            if (currencyRelativeTo == "USD")
            {
                data = currencyData;
            }
            else if (currency == "USD")
            {
                data = currencyRelativeToData.Select(a => Math.Abs(a) < 0.000001 ? double.NaN : 1.0 / a).ToArray();
            }
            else
            {
                data = new double[currencyData.Length];
                for (int i = 0; i < currencyData.Length; ++i)
                {
                    var vl = currencyData[i] / currencyRelativeToData[i];
                    if (double.IsNaN(vl) || double.IsInfinity(vl))
                    {
                        data[i] = double.NaN;
                    }
                    else
                    {
                        data[i] = vl;
                    }
                }
            }

            var result = new Dictionary<string, object>();
            result.Add(
                "series",
                new Dictionary<string, string>
                {
                    {"metric", "currency_pair"},
                    {"name", currency + currencyRelativeTo},
                    {"currency", currency},
                    {"currency_relative_to", currencyRelativeTo}
                }
            );
            result.Add(
                "values",
                data.Select(a => a.ToString()).ToList()
            );
            var r = new List<object> { result };
            sw.Write(JsonConvert.SerializeObject(r));
        }


        private void HandleSeriesSetRequest(StreamWriter sw, Dictionary<string, string> seriesSetSpec)
        {
            string currency = null;
            string currencyRelativeTo = null;
            if (seriesSetSpec.ContainsKey("currency")) { currency = seriesSetSpec["currency"]; }
            if (seriesSetSpec.ContainsKey("currency_relative_to")) { currencyRelativeTo = seriesSetSpec["currency_relative_to"]; }

            var seriesSet = _dataCache.CurrencyPairs.Where(a =>
                (currency == null || a.Currency == currency) &&
                (currencyRelativeTo == null || a.CurrencyRelativeTo == currencyRelativeTo)).ToList();

            var result = seriesSet.Select(a =>
                   new Dictionary<string, Dictionary<string, string>> {
                        {
                            "series",
                            new Dictionary<string, string>
                            {
                                {"metric", "currency_pair"},
                                {"name", a.Currency + a.CurrencyRelativeTo},
                                {"currency", a.Currency},
                                {"currency_relative_to", a.CurrencyRelativeTo}
                            }
                        }
                   }
            ).ToList();

            sw.Write(JsonConvert.SerializeObject(result));
        }

        private string HandleLvs()
        {
            var result = new Dictionary<string, Dictionary<string, string>>();
            result.Add("currency", new Dictionary<string, string>());
            result.Add("currency_relative_to", new Dictionary<string, string>());
            result.Add("name", new Dictionary<string, string>());

            result.Add("metric", new Dictionary<string, string> { { "currency_pair", ((_dataCache.Currencies.Count) * (_dataCache.Currencies.Count - 1)).ToString() } });
            for (int i = 0; i < _dataCache.Currencies.Count; ++i)
            {
                var currency = (string)_dataCache.Currencies[i];
                result["currency"].Add(currency, (_dataCache.Currencies.Count - 1).ToString());
                result["currency_relative_to"].Add(currency, (_dataCache.Currencies.Count - 1).ToString());
                for (int j = 0; j < _dataCache.Currencies.Count; ++j)
                {
                    if (i == j) { continue; }
                    var relativeTo = (string)_dataCache.Currencies[j];
                    result["name"].Add(currency + relativeTo, 1.ToString());
                }
            }

            return JsonConvert.SerializeObject(result);
        }

        public void HandleRequest(HttpContext context, StreamWriter sw)
        {
            var request = context.Request;

            if (((string)request.Path).StartsWith("/api/v1/series"))
            {
                var parameters = request.Query.ToDictionary(q => q.Key, q => q.Value[0]);

                // query parameter must be present.
                if (!parameters.ContainsKey("query"))
                {
                    sw.Write(JsonConvert.SerializeObject(new List<object>()));
                    return;
                }
                
                // series specified must 
                var ss = parseSeriesSet(parameters["query"]);
                if (ss.Count == 0) 
                {
                    sw.Write(JsonConvert.SerializeObject(new List<object>()));
                    return;
                }
                
                long start = long.MinValue;
                long stop = long.MinValue;
                long step = long.MinValue; 
                
                if (parameters.ContainsKey("start")) { long.TryParse(parameters["start"], out start); }
                if (parameters.ContainsKey("stop")) { long.TryParse(parameters["stop"], out stop); }
                if (parameters.ContainsKey("step")) { long.TryParse(parameters["step"], out step); }

                if (start != long.MinValue && stop != long.MinValue && step != long.MinValue)
                {
                    HandleDataRequest(sw, ss, start, stop, step);
                }                                                
                else
                {
                    HandleSeriesSetRequest(sw, ss);
                }
            }
            else if (request.Path == "/api/v1/functions")
            {
                sw.Write("[]");
            }
            else if (request.Path == "/api/v1/aggregation-functions")
            {
                sw.Write("[]");   
            }
            else if (request.Path == "/api/v1/label-and-value-summary")
            {
                sw.Write(HandleLvs());
            }
            else if (request.Path == "/api/v1/predefined-pages")
            {  
                sw.Write("[]");
            }
            else 
            {
                sw.Write("404 not found");
                context.Response.StatusCode = 404;
            }
        }

        public void Configure(IApplicationBuilder app, IHostingEnvironment env)
        {            
            app.Use(async (context, next) =>
                {
                    try
                    {
                        context.Response.Headers.Add("Content-Type", "application/json; charset=utf-8");
                        // TODO: this should be locked down:
                        context.Response.Headers.Add("Access-Control-Allow-Origin", "*");
                        // TODO: reconsider the need for no-caching:
                        context.Response.Headers.Add("Cache-Control", "no-cache, no-store, must-revalidate");
                        context.Response.Headers.Add("Pragma", "no-cache");
                        context.Response.Headers.Add("Expires", "0");

                        context.Response.StatusCode = 200;

                        if (context.Request.Method != "GET")
                        {
                            context.Response.StatusCode = 501;
                            return;
                        }
                        
                        // Construct response in a MemoryStream rather than writing directly to ctx.Response.Body
                        // so that a 501 can be sent in the event of an exception.
                        using (var ms = new MemoryStream())
                        {
                          using (var sw = new StreamWriter(ms))
                          {
                              HandleRequest(context, sw);
                          }
                          var bs = ms.ToArray();
                          context.Response.Body.Write(bs, 0, bs.Length);
			            }
                    }
                    catch (Exception e)
                    {
                        context.Response.StatusCode = 500;
                        // TODO: better logging.
                        Console.WriteLine("Request for url: " + context.Request.Path + " resulted in exception: " + e.Message);
                        using (var sw = new StreamWriter(context.Response.Body))
                        {
                            var t = sw.WriteLineAsync("internal server error");
                            t.Wait();
                        }
                    }

                    await next();
                });
        }
    }
}
