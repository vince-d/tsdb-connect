package edu.teco.tsdbwriter;

import android.os.AsyncTask;
import android.support.v7.app.ActionBarActivity;
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


public class MainActivity extends ActionBarActivity {

    private static final String TAG = "TSDBWriter";

    @Override
    protected void onResume() {
        super.onResume();

        // rate: sample data incrementally (true or false).
        // downsample: ex: "5m-avg" (has s, h, d, etc.)
        // start / end can be relative ("24h-ago") or absolute (UNIX timestamp)

        QueryTSDBTask queryts = new QueryTSDBTask();
        queryts.execute(1,2,3,4);



        // Start async task.
        //PutIntoTSDBTask tsdb = new PutIntoTSDBTask();
        //tsdb.execute(1,2,3,4);
    }

    private class PutIntoTSDBTask extends AsyncTask<Integer, String, String> {


        @Override
        protected String doInBackground(Integer... vals) {

            // Write all the data.
            for (int value : vals) {
                String result = putJsonAndGetResult(value);
                publishProgress(result);
            }

            return "All PUTs done";
        }

        @Override
        protected void onProgressUpdate(String... result) {
            super.onProgressUpdate(result);

            Log.d(TAG, "JSON PUT done: " + result[0]);
        }

        private String putJsonAndGetResult(int value) {
            InputStream inputStream = null;
            String result = "";


            HttpClient httpclient = new DefaultHttpClient();

            // Connection parameters
            String server = "cumulus.teco.edu";
            String port = "52001";
            String loc = "data";

            // ID of bPart. Can be anything, but MAC address might be a good idea.
            // The server puts this to the tag "resource_id".
            // This means you can easily filter your time series for a certain metric by this ID if
            // you only want to get the data supplied by this bPart.
            String bPartID = "MyDeviceID2";

            // Put together URL.
            HttpPut httpPUT = new HttpPut("http://" + server + ":" + port + "/" + loc + "/" + bPartID + "/");


            JSONObject main = new JSONObject();
            JSONObject energy = new JSONObject();
            JSONObject data = new JSONObject();

            // Calculate UNIX timestamp.
            Long tsLong = System.currentTimeMillis() / 1000;

            try {
                // Data. Contains the data and tags.
                main.put("data", energy);

                // Put metric.
                // The type of data. Can be any string, like "temperature" or "pressure".
                energy.put("SomeData", data);

                // Put value.
                // This is needed.
                data.put("value", value);

                // Put tags.
                // Those are optional and can be used to filter the data when placing a query.
                data.put("unit", "degrees");
                data.put("cats", "none");

                // Put timestamp.
                // You can use any timestamp. This means you may also upload data from
                // the past or the future.
                main.put("stime", tsLong);

                // Put device ID.
                // This is written as value for the tag "resource_id"
                main.put("id", bPartID);
            } catch (JSONException e) {
                Log.e(TAG, "Malformed JSON.");
            }

            String json;
            json = main.toString();

            //Log.d("JSONSent", json);

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

        /** The system calls this to perform work in the UI thread and delivers
         * the result from doInBackground() */
        protected void onPostExecute(String result) {
            Log.d(TAG, result);
        }
    }


    private class QueryTSDBTask extends AsyncTask<Integer, String, String> {


        @Override
        protected String doInBackground(Integer... vals) {

            String result = putJsonAndGetResult();


            return result;
        }

        @Override
        protected void onProgressUpdate(String... result) {
            super.onProgressUpdate(result);

            Log.d(TAG, "JSON PUT done: " + result[0]);
        }

        private String putJsonAndGetResult() {
            InputStream inputStream = null;
            String result = "";


            HttpClient httpclient = new DefaultHttpClient();

            // Connection parameters
            String server = "cumulus.teco.edu";
            String port = "52001";
            String loc = "data";

            // ID of bPart. Can be anything, but MAC address might be a good idea.
            // The server puts this to the tag "resource_id".
            // This means you can easily filter your time series for a certain metric by this ID if
            // you only want to get the data supplied by this bPart.
            String bPartID = "MyDeviceID2";

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

        /** The system calls this to perform work in the UI thread and delivers
         * the result from doInBackground() */
        protected void onPostExecute(String result) {
            Log.d(TAG, result);
        }
    }
}
