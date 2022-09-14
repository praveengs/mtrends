package com.home.mtrends.service;

import com.home.mtrends.dao.DataReader;
import com.home.mtrends.model.PlayCount;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.types.DataTypes;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.List;

import static org.apache.spark.sql.functions.*;

@Service
public class TrendsService {

    private final DataReader dataReader;

    private final int topNSessionCount;
    private final int timePeriodBetweenSessions;
    private final int topNSongCount;


    public TrendsService(DataReader dataReader, @Value("${mtrends.top-session-count:50}") int topNSessionCount,
                         @Value("${mtrends.time-between-sessions-in-minutes:20}") int timePeriodBetweenSessionsInMinutes,
                         @Value("${mtrends.top-song-count:10}") int topNSongCount) {
        this.dataReader = dataReader;
        this.topNSessionCount = topNSessionCount;
        this.timePeriodBetweenSessions = timePeriodBetweenSessionsInMinutes * 60;
        this.topNSongCount = topNSongCount;
    }

    public List<PlayCount> getTrends() {

        // Get the user sessions
        Dataset<Row> userSessionsDataset = extractUserSessionsDataSet();

//        userSessionsDataset
//                .select("userId", "timestamp", "mbTrackId", "prevStartTime", "datedifference", "uniqueSessionId")
//                .show(100, false);

        // Find the top n sessions
        Dataset<Row> topNSessions = findTopNSessions(userSessionsDataset);
//        topNSessions.show(20,false);

        // Find the top n songs from the set of sessions.
        Dataset<Row> topNSongs = findTopNSongsFromSessions(userSessionsDataset, topNSessions);

//        topNSongs.show(50, false);

        // Map it to a POJO and return the list.
        return topNSongs
                .as(Encoders.bean(PlayCount.class))
                .collectAsList();
    }

    /**
     * Find the top songs from the input dataset of top n sessions, by joining it with the set of user sessions.
     *
     * @param userSessionsDataset the persisted user sessions data set.
     * @param topNSessions        the top identified sessions with the longest listening times.
     * @return top n songs from the top sessions.
     */
    private Dataset<Row> findTopNSongsFromSessions(Dataset<Row> userSessionsDataset, Dataset<Row> topNSessions) {
        // find the top n songs by
        return topNSessions
                // joining with the user sessions using the session id
                .join(userSessionsDataset, "uniqueSessionId")
                // grouping with the track name, as track id is null at times.
                .groupBy("trackName")
                // creating a count for the number of times the track appears
                .agg(count("trackName").as("playedCount"))
                // order by the count in descending order
                .orderBy(col("playedCount").desc())
                // and taking the top n values
                .limit(topNSongCount);
    }

    /**
     * Extract the top n sessions from the user session data set.
     */
    private Dataset<Row> findTopNSessions(Dataset<Row> userSessionsDataset) {
        // Extract the top n sessions from the sessions data set by
        return userSessionsDataset
                // 1. grouping on the session id
                .groupBy("uniqueSessionId")
                .agg(max(col("timestamp")).as("lastTrackTime"),
                        min(col("timestamp")).as("firstTrackTime"))
                .withColumn("sessionTime", col("lastTrackTime")
                        .minus(col("firstTrackTime")).cast(DataTypes.LongType))
                .orderBy(col("sessionTime").desc())
//
//                // 2. Find the number of tracks played in each session
//                .agg(count("trackName").as("numberTracksPlayedInSession"))
//                // 3. order sessions by count of tracks
//                .orderBy(col("numberTracksPlayedInSession").desc())
                // 4. take the first n rows
                .limit(topNSessionCount);
    }

    /**
     * Extract and persist the user sessions data set.
     */
    private Dataset<Row> extractUserSessionsDataSet() {
        // the window over which the data will be partitioned, which is the user id, and ordered by timestamp
        WindowSpec windowSpec = Window.partitionBy("userId").orderBy("timestamp");

        // find the previous timestamp of the current played track over the partitioned data set for user.
        Column previousTimestamp = when(lag(col("timestamp"), 1)
                .over(windowSpec).isNull(), col("timestamp"))
                .otherwise(lag(col("timestamp"), 1)
                        .over(windowSpec));

        // find the difference between the previous track played time and the current track played time
        // and check if the difference is greater than or equal to specified time period for a session.
        Column dateDifference = col("timestamp")//.cast(DataTypes.LongType)
                .minus(col("prevStartTime")).cast(DataTypes.LongType)
                .geq(timePeriodBetweenSessions);

        // Create a unique session id for the user, using
        // 1. user id
        // 2. sum of the columns so far for the row in which a new session was identified
        Column uniqueSessionId =
                concat(col("userId"), lit("_"), sum("datedifference").over(windowSpec));


        return dataReader.readData()
                .withColumn("prevStartTime", previousTimestamp)
                //.withColumn("timestamp_val", to_timestamp(col("timestamp")))
                //.withColumn("prevtimestamp_val", to_timestamp(col("prevStartTime")))
                //.withColumn("datediff", Column("timestamp_val").cast(DataTypes.LongType) - col("prevtimestamp_val").cast(DataTypes.LongType));
                .withColumn("datedifference", dateDifference.cast(DataTypes.IntegerType))
                .withColumn("uniqueSessionId", uniqueSessionId)
                .persist();
    }
}
