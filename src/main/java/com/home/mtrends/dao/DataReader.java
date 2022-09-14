package com.home.mtrends.dao;

import com.home.mtrends.model.UserTrackRecord;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public interface DataReader {
    Dataset<UserTrackRecord> readData();
}
