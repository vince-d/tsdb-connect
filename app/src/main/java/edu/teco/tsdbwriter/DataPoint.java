package edu.teco.tsdbwriter;

/**
 * Created by Vincent on 04.01.2015.
 */


/**
 * A single data point in a time series.
 */
public class DataPoint<T>{

    private String mValue;

    private String mTimestamp;

    public DataPoint(long timestamp, T value) {
        mValue = value.toString();
        mTimestamp = String.valueOf(timestamp);
    }
}
