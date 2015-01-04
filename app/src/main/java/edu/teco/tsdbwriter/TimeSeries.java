package edu.teco.tsdbwriter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Vincent on 04.01.2015.
 */
public class TimeSeries<T> {


    private String mMetric;

    private String mDeviceID;

    private Map<String, String> mTags;

    private List<DataPoint> mDataPoints;

    public TimeSeries(String metric, String deviceID) {
        mMetric = metric;

        if (mDeviceID != null)
            mDeviceID = deviceID;
        else
            mDeviceID = "";

        mTags = new HashMap<String, String>();
        mDataPoints = new ArrayList<DataPoint>();
    }

    public void addDataPoint(long timestamp, T value) {
        DataPoint<T> newDataPoint = new DataPoint<T>(timestamp, value);
        mDataPoints.add(newDataPoint);
    }


    /**
     * Put key-value-pair into the tag list for the time series.
     * Uses toString of value to get the string value for storage.
     * @param key The key.
     * @param value The value.
     */
    public void addTag(String key, Object value) {
        String strValue = value.toString();
        mTags.put(key, strValue);
    }

    // Helpers for handling basic types (non-object).
    /**
     * Put key-value-pair into the tag list for the time series.
     * @param key The key.
     * @param value The integer value.
     */
    public void addTag(String key, int value) {
        addTag(key, Integer.valueOf(value));
    }

    /**
     * Put key-value-pair into the tag list for the time series.
     * @param key The key.
     * @param value The long value.
     */
    public void addTag(String key, long value) {
        addTag(key, Long.valueOf(value));
    }

    /**
     * Put key-value-pair into the tag list for the time series.
     * @param key The key.
     * @param value The float value.
     */
    public void addTag(String key, float value) {
        addTag(key, Float.valueOf(value));
    }

    /**
     * Put key-value-pair into the tag list for the time series.
     * @param key The key.
     * @param value The double value.
     */
    public void addTag(String key, double value) {
        addTag(key, Double.valueOf(value));
    }

    /**
     * Put key-value-pair into the tag list for the time series.
     * @param key The key.
     * @param value The boolean value.
     */
    public void addTag(String key, boolean value) {
        addTag(key, Boolean.valueOf(value));
    }
}
