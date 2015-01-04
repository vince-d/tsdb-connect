package edu.teco.tsdbwriter;

import android.os.AsyncTask;
import android.util.Log;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Created by Vincent on 04.01.2015.
 */
public class TSDB {

    // The URL for writing data to the TSDB.
    private String mWriteURL;

    // The URL for querying data from the TSDB
    private String mQueryURL;

    public TSDB(String writeURL, String queryURL) {
        mWriteURL = writeURL;
        mQueryURL = queryURL;
    }

    public void write(TimeSeries timeSeries) {
        // Start async task.
        PutIntoTSDBTask tsdbWrite = new PutIntoTSDBTask();
        tsdbWrite.execute(timeSeries);
    }

    public void query(Query query) {
        QueryTSDBTask tsdbRead = new QueryTSDBTask();
        tsdbRead.execute(query);
    }

    // Needed for logging.
    private String convertStreamToString(InputStream is) {
        BufferedReader reader = new BufferedReader(new InputStreamReader(is));
        StringBuilder sb = new StringBuilder();

        String line = null;
        try {
            while ((line = reader.readLine()) != null) {
                sb.append(line + "\n");
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                is.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return sb.toString();
    }


    private class PutIntoTSDBTask extends AsyncTask<TimeSeries, String, String> {


        @Override
        protected String doInBackground(TimeSeries... timeSeries) {

            List<DataPoint> dataPoints = timeSeries[0].getDataPoints();

            // Write all the data.
            for (DataPoint dp : dataPoints) {
                String result = putJsonAndGetResult(timeSeries[0], dp);
                publishProgress(result);
            }

            return "All PUTs done";
        }

        @Override
        protected void onProgressUpdate(String... result) {
            super.onProgressUpdate(result);

            Log.d(MainActivity.TAG, "JSON PUT done: " + result[0]);
        }

        private String putJsonAndGetResult(TimeSeries ts, DataPoint dp) {
            InputStream inputStream = null;
            String result = "";


            HttpClient httpclient = new DefaultHttpClient();

            // ID of bPart. Can be anything, but MAC address might be a good idea.
            // The server puts this to the tag "resource_id".
            // This means you can easily filter your time series for a certain metric by this ID if
            // you only want to get the data supplied by this bPart.


            // Put together URL.
            HttpPut httpPUT = new HttpPut(mWriteURL + ts.getDeviceID() + "/");


            JSONObject main = new JSONObject();
            JSONObject metric = new JSONObject();
            JSONObject tags = new JSONObject();


            try {
                // Data. Contains the data and tags.
                main.put("data", metric);

                // Put metric.
                // The type of data. Can be any string, like "temperature" or "pressure".
                metric.put(ts.getMetric(), tags);

                // Put value.
                // This is needed.
                tags.put("value", dp.getValue());

                // Put tags.
                // Those are optional and can be used to filter the data when placing a query.
                Map mp = ts.getTags();
                Iterator it = mp.entrySet().iterator();
                while (it.hasNext()) {
                    Map.Entry pairs = (Map.Entry)it.next();
                    tags.put(pairs.getKey().toString(), pairs.getValue().toString());
                }

                // Put timestamp.
                // You can use any timestamp. This means you may also upload data from
                // the past or the future.
                main.put("stime", dp.getTimestamp());

                // Put device ID.
                // This is written as value for the tag "resource_id"
                main.put("id", ts.getDeviceID());
            } catch (JSONException e) {
                Log.e(MainActivity.TAG, "Malformed JSON.");
            }

            String json;
            json = main.toString();

            Log.d("JSONSent", json);

            try {
                // PUT data to server and read response.
                StringEntity se = new StringEntity(json);
                httpPUT.setEntity(se);

                HttpResponse httpResponse = httpclient.execute(httpPUT);
                inputStream = httpResponse.getEntity().getContent();

                if(inputStream != null) {
                    result = convertStreamToString(inputStream);
                } else {
                    result = "Exception while writing.";
                }

            } catch (IOException e) {
                Log.d("InputStream", e.getLocalizedMessage());
            }

            return result;
        }

        /** The system calls this to perform work in the UI thread and delivers
         * the result from doInBackground() */
        protected void onPostExecute(String result) {
            Log.d(MainActivity.TAG, result);
        }
    }


    private class QueryTSDBTask extends AsyncTask<Query, String, String> {


        @Override
        protected String doInBackground(Query... query) {

            String result = queryStringOverHttpGet(query[0]);


            return result;
        }

        @Override
        protected void onProgressUpdate(String... result) {
            super.onProgressUpdate(result);

            Log.d(MainActivity.TAG, "JSON PUT done: " + result[0]);
        }

        private String queryStringOverHttpGet(Query q) {
            InputStream inputStream = null;
            String result = "";


            HttpClient httpclient = new DefaultHttpClient();

            // Put together URL.
            String base = "http://cumulus.teco.edu:4242/api/query?";
            base += "start=10h-ago" + "&";
            base += "end=1s-ago" + "&";
            base += "m=sum:SomeData";

            try {
                base += URLEncoder.encode("{unit=degrees,resource_id=MyDeviceID2,cats=*}", "UTF8");
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
            }

            HttpGet httpGET = new HttpGet(base);


            try {
                // PUT data to server and read response.

                HttpResponse httpResponse = httpclient.execute(httpGET);
                inputStream = httpResponse.getEntity().getContent();

                if(inputStream != null) {
                    result = convertStreamToString(inputStream);
                } else {
                    result = "Exception while writing.";
                }

            } catch (IOException e) {
                Log.d("InputStream", e.getLocalizedMessage());
            }

            return result;
        }

        /** The system calls this to perform work in the UI thread and delivers
         * the result from doInBackground() */
        protected void onPostExecute(String result) {
            Log.d(MainActivity.TAG, result);
        }
    }
}
