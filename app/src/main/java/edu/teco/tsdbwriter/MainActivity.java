package edu.teco.tsdbwriter;

// Author: Vincent Diener  -  diener@teco.edu

import android.support.v7.app.ActionBarActivity;

public class MainActivity extends ActionBarActivity {

    public static final String TAG = "TSDBWriterLog";

    @Override
    protected void onResume() {
        super.onResume();

        // Base URLs for writing and reading data to the TSDB.
        String writeURL = "<WRITE URL GOES HERE>";
        String queryURL = "<READ URL GOES HERE>";

        // Create TSDB object.
        TSDB tsdb = new TSDB(writeURL, queryURL);

        // Create time series with value type Double.
        // The data we have is of the type "MyCoolMetric" (this could be something like "light").
        // The device that recorded the data was "MyDevice".
        TimeSeries<Double> timeSeries = new TimeSeries<>("MyCoolMetric", "MyDevice");

        // Add some data points to the time series. Timestamp is current time + x.
        long currentTime = System.currentTimeMillis() / 1000;
        timeSeries.addDataPoint(currentTime + 0, 1.0);
        timeSeries.addDataPoint(currentTime + 1, 2.0);
        timeSeries.addDataPoint(currentTime + 2, 3.0);
        timeSeries.addDataPoint(currentTime + 3, 4.0);
        timeSeries.addDataPoint(currentTime + 4, 5.0);

        // Add two tags to the timeseries.
        // Those key-value pairs can be any string.
        timeSeries.addTag("SomeTag", "SomeValue");
        timeSeries.addTag("SomeOtherTag", "SomeOtherValue");

        // Write all the data to the TSDB.
        tsdb.write(timeSeries);

        // Next, we want to read the data we just wrote.

        // First, create a new timeseries to put our query result into.
        // Null means we want to query data not from one specific device but all data that has
        // the metric we are looking for.
        TimeSeries<Double> newTimeSeries = new TimeSeries<>("MyCoolMetric", null);

        // Only return data points that have "SomeTag" set to "SomeValue".
        // The value of "SomeOtherTag" is ignored in the query.
        newTimeSeries.addTag("SomeTag", "SomeValue");

        // See documentation of Query.java for explanation of parameters.
        currentTime = System.currentTimeMillis() / 1000;
        Query query = new Query(newTimeSeries, String.valueOf(currentTime - 60), String.valueOf(currentTime + 60), "avg", null);

        // Start query.
        // Note that the returned data points are not written to the newTimeSeries right now, but just
        // debug-printed as JSON-String. The JSON-parsing will be implemented later.
        tsdb.query(query);

    }





}
