package edu.teco.tsdbwriter;

import android.support.v7.app.ActionBarActivity;


public class MainActivity extends ActionBarActivity {

    public static final String TAG = "TSDBWriterLog";

    @Override
    protected void onResume() {
        super.onResume();

        // rate: sample data incrementally (true or false).
        // downsample: ex: "5m-avg" (has s, h, d, etc.)
        // start / end can be relative ("24h-ago") or absolute (UNIX timestamp)

        //QueryTSDBTask queryts = new QueryTSDBTask();
        //queryts.execute(1,2,3,4);

        String writeURL = "http://cumulus.teco.edu:52001/data/";
        String queryURL = "http://cumulus.teco.edu:4242/api/query";

        TSDB tsdb = new TSDB(writeURL, queryURL);

        TimeSeries<Double> timeSeries = new TimeSeries<>("MyCoolMetric", "MyDevice");

        long currentTime = System.currentTimeMillis() / 1000;

        timeSeries.addDataPoint(currentTime + 0, 1.0);
        timeSeries.addDataPoint(currentTime + 1,2.0);
        //timeSeries.addDataPoint(currentTime + 2, 3.0);
        //timeSeries.addDataPoint(currentTime + 3, 4.0);
        //timeSeries.addDataPoint(currentTime + 4, 5.0);
        timeSeries.addTag("SomeTag", "SomeValue");
        timeSeries.addTag("SomeOtherTag", "SomeOtherValue");

        tsdb.write(timeSeries);

        // Null means we want to query data not from one specific device but all data that has
        // the metric we are looking for.
        TimeSeries<Double> newTimeSeries = new TimeSeries<>("MyCoolMetric", null);

        // Only return data points that have "SomeTag" set to "SomeValue".
        // The value of "SomeOtherTag" is ignored in the query.
        newTimeSeries.addTag("SomeTag", "SomeValue");

        Query query = new Query(timeSeries, "5m-ago", String.valueOf(currentTime + 30), "avg", null);

        // See documentation of Query.java for explanation of parameters.
        tsdb.query(query);

    }





}
