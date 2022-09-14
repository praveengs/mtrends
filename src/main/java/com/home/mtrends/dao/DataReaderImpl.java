package com.home.mtrends.dao;

import com.home.mtrends.model.UserTrackRecord;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Repository;

@Repository
public class DataReaderImpl implements DataReader {
    private final SparkSession sparkSession;
    private final String inputFilePath;
    public DataReaderImpl(SparkSession sparkSession,
                          @Value("${mtrends.playedtrack.input.file.path}") String inputFilePath) {
        this.sparkSession = sparkSession;
        this.inputFilePath = inputFilePath;
    }

    @Override
    public Dataset<UserTrackRecord> readData() {
        StructType schema = new StructType()
                .add("userId", DataTypes.StringType)
                .add("timestamp", DataTypes.TimestampType)
                .add("mbid", DataTypes.StringType)
                .add("artistName", DataTypes.StringType)
                .add("mbTrackId", DataTypes.StringType)
                .add("trackName", DataTypes.StringType);
        Encoder<UserTrackRecord> encoder = Encoders.bean(UserTrackRecord.class);
        Dataset<UserTrackRecord> userTrackRecordDataset = sparkSession
                .read()
                .option("sep", "\t")
                .option("header", false)
                .schema(schema)
                .csv(inputFilePath).as(encoder);
//        userTrackRecordDataset.show();
        return userTrackRecordDataset;
    }
}
