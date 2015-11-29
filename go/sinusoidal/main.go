// Implments a simple custom data server for http://www.datavstime.com.
// Includes capability to handle a function and an aggregate function.
// Minimal implementation - not a lot of error checking etc.
package main

import (
	"fmt"
	"io"
	"log"
	"math"
	"strconv"
	"strings"
	"encoding/json"
	"net/http"
)

var allSeries = []map[string]string {
	{"metric": "sin", "period": "10", "scale": "small", "group": "all"},
	{"metric": "sin", "period": "20", "scale": "small", "group": "all"},
	{"metric": "sin", "period": "30", "scale": "small", "group": "all"},
	{"metric": "sin", "period": "40", "scale": "medium", "group": "all"},
	{"metric": "sin", "period": "50", "scale": "medium", "group": "all"},
	{"metric": "sin", "period": "60", "scale": "medium", "group": "all"},
	{"metric": "sin", "period": "70", "scale": "medium", "group": "all"},
	{"metric": "sin", "period": "80", "scale": "large", "group": "all"},
	{"metric": "sin", "period": "90", "scale": "large", "group": "all"},
	{"metric": "sin", "period": "100", "scale": "large", "group": "all"},
}

var allFunctions = []string { "derivative" }

var allAggregationFunctions = []string { "sum" }

func main() {	
	fmt.Println("starting...")
	http.HandleFunc("/api/v1/functions", handleFunctionsRequest)
	http.HandleFunc("/api/v1/aggregation-functions", handleAggregationFunctionsRequest)
	http.HandleFunc("/api/v1/predefined-pages", handlePredefinedPagesRequest)
	http.HandleFunc("/api/v1/series", handleQueryRequest)
	http.HandleFunc("/api/v1/label-and-value-summary", handleLvsRequest)	
	log.Fatal(http.ListenAndServe("localhost:7766", nil))
}

func handleLvsRequest(w http.ResponseWriter, r *http.Request) {
	addHeaders(w)
		
	labels := make(map[string]map[string]int)
	
	for _, series := range allSeries {
		for k, v := range series {
			if _, present := labels[k]; !present {
				labels[k] = make(map[string]int)
			}
			labels[k][v] += 1
		}
	}
	
	result, _ := json.Marshal(labels)
	io.WriteString(w, string(result))
}

func extractFunction(s string) (string, string) {
	if strings.HasSuffix(s, ")") {
		body := s[strings.Index(s, "(")+1:len(s)-1]
		fName := s[0:strings.Index(s, "(")]
		return fName, body
	}
	return "", s
}

func extractFunctionParameter(s string) (string, string) {
	if strings.HasSuffix(s, "]") {
		body := s[0:strings.Index(s, "[")]
		wSpec := s[strings.Index(s, "[")+1:len(s)-1]
		return wSpec, body
	}
	return "", s
}

func parseSeriesSpecString(s string) map[string]string {
	if !strings.HasSuffix(s, "}") || !strings.HasPrefix(s, "{") {
		return make(map[string]string)
	}
	parts := strings.Split(s[1:len(s)-1], ",")
	
	result := make(map[string]string)
	for _, part := range parts {
		kv := strings.SplitN(part,":",2)
		if len(kv) != 2 {
			continue 
		}
		if !strings.HasSuffix(kv[1], "'") || !strings.HasPrefix(kv[1], "'") {
			continue
		}
		result[kv[0]] = kv[1][1:len(kv[1])-1]
	}
	return result
}

func parseQueryString(s string) (applyFunc bool, applyAggregateFunc bool, seriesSetSpec map[string]string) {	
	func1, body := extractFunction(s)
	func2, body := extractFunction(body)
	// Note: ignoring function parameter for purposes of this demo.
	_, body = extractFunctionParameter(body)
	
	applyFunc = func1 == allFunctions[0] || func2 == allFunctions[0]
	applyAggregateFunc = func1 == allAggregationFunctions[0]
	seriesSetSpec = parseSeriesSpecString(body)
	return applyFunc, applyAggregateFunc, seriesSetSpec
}

func constructSeriesSet(seriesSetSpec map[string]string) []map[string]map[string]string {
	seriesSet := make([]map[string]map[string]string, 0)
	
	for _, series := range allSeries {
		hasAll := true
		for key, val := range seriesSetSpec {
			if _, ok := series[key]; !ok {
				hasAll = false
				continue
			}
			if series[key] != val {
				hasAll = false
				continue
			}
		} 
		if hasAll {
			seriesDef := make(map[string]map[string]string)
			seriesDef["series"] = series 
			seriesSet = append(seriesSet, seriesDef)
		}
	}
	return seriesSet
}

func handleSeriesSetRequest(w http.ResponseWriter, seriesSetSpec map[string]string) {
	result, _ := json.Marshal(constructSeriesSet(seriesSetSpec))
	io.WriteString(w, string(result))
}

type DataResult struct {
	Series map[string]string	`json:"series"`
	Values []float64			`json:"values"`
}

func handleDataRequest(
	w http.ResponseWriter, 
	seriesSetSpec map[string]string, 
	applyFunc bool, 
	applyAggregateFunc bool, 
	start int, stop int, step int) {

	seriesSet := constructSeriesSet(seriesSetSpec)
	if len(seriesSet) != 1 {
		// todo: error handling
		return
	}

	series := seriesSet[0]["series"]
	period, _ := strconv.ParseFloat(series["period"], 64)
	
	nPoints := (stop-start)/step;
	if (stop-start) % step == 0 {
		nPoints += 1
	} 
	
	vals := make([]float64, nPoints)
	
	for i := 0; i <nPoints; i++ {
		vals[i] = math.Sin(2.0*math.Pi*float64(start + i*step)/(period*1000.0))
	}

	result, _ := json.Marshal([]DataResult {{ series, vals }})
	io.WriteString(w, string(result))
}

func handleQueryRequest(w http.ResponseWriter, r *http.Request) {
	addHeaders(w)
	
	series := r.URL.Query().Get("query")
	if series == "" {
		io.WriteString(w, "[]") 
		return
	}
	
	applyFunc, applyAggregateFunc, seriesSetSpec := parseQueryString(series)
	
	start := r.URL.Query().Get("start")
	stop := r.URL.Query().Get("stop")
	step := r.URL.Query().Get("step")

	if start == "" || stop == "" || step == "" {
		handleSeriesSetRequest(w, seriesSetSpec)
	} else {
		start_f, _ := strconv.Atoi(start)
		stop_f, _ := strconv.Atoi(stop)
		step_f, _ := strconv.Atoi(step)
		handleDataRequest(w, seriesSetSpec, applyFunc, applyAggregateFunc, start_f, stop_f, step_f)
	}
}

func handlePredefinedPagesRequest(w http.ResponseWriter, r *http.Request) {
	addHeaders(w)
	io.WriteString(w, "[]");
}

func handleFunctionsRequest(w http.ResponseWriter, r *http.Request) {
	addHeaders(w)
	result, _ := json.Marshal(allFunctions)
	io.WriteString(w, string(result))
}

func handleAggregationFunctionsRequest(w http.ResponseWriter, r *http.Request) {
	addHeaders(w)
	result, _ := json.Marshal(allAggregationFunctions)
	io.WriteString(w, string(result))
}

func addHeaders(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	// cross origin requests must be explicitly allowed.
	w.Header().Set("Access-Control-Allow-Origin", "*")
	// ensure that no caching happens anywhere.
	w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")
	w.Header().Set("Pragma", "no-cache")
	w.Header().Set("Expires", "0")
}