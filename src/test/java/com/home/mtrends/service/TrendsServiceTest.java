package com.home.mtrends.service;

import com.home.mtrends.dao.DataReader;
import com.home.mtrends.model.PlayCount;
import com.home.mtrends.model.UserTrackRecord;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Locale;

import static org.mockito.Mockito.when;

@ExtendWith({MockitoExtension.class})
class TrendsServiceTest {
    private static SparkSession spark;

    @Mock
    private DataReader mockDataReader;

    private TrendsService trendsService;

    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'", Locale.ENGLISH);


    @BeforeAll
    static void setup() {
        spark = SparkSession.builder()
                .master("local[*]")
                .getOrCreate();
    }

    @BeforeEach
    void setUp() {
        trendsService = new TrendsService(mockDataReader, 2, 5, 2);
    }

    @Test
    void testHappyPathWithANumberofTracksOverANumberOfSessions() {
//        LocalDateTime now = LocalDateTime.now().truncatedTo(ChronoUnit.SECONDS);
        Instant now = Instant.now().truncatedTo(ChronoUnit.SECONDS);
        StructType schema = new StructType()
                .add("userId", DataTypes.StringType)
                .add("timestamp", DataTypes.TimestampType)
                .add("mbid", DataTypes.StringType)
                .add("artistName", DataTypes.StringType)
                .add("mbTrackId", DataTypes.StringType)
                .add("trackName", DataTypes.StringType);

        List<Row> userTrackRecordsRows = List.of(
                // first session for user1
                RowFactory.create("user1", now, null, null, null, "track1"),
                RowFactory.create("user1", now.minusSeconds(3 * 60), null, null, null, "track1"),
                RowFactory.create("user1", now.minusSeconds(6 * 60), null, null, null, "track1"),
                RowFactory.create("user1", now.minusSeconds(9 * 60), null, null, null, "track2"),
                RowFactory.create("user1", now.minusSeconds(12 * 60), null, null, null, "track3"),
                // second session for user 1
                RowFactory.create("user1", now.minusSeconds(60 * 60), null, null, null, "track4"),
                RowFactory.create("user1", now.minusSeconds(63 * 60), null, null, null, "track5"),
                // First session from user 2
                RowFactory.create("user2", now, null, null, null, "track1"),
                RowFactory.create("user2", now.minusSeconds(3 * 60), null, null, null, "track2"),
                RowFactory.create("user2", now.minusSeconds(6 * 60), null, null, null, "track1"),
                // First session from user 3
                RowFactory.create("user3", now, null, null, null, "track2"),
                RowFactory.create("user3", now.minusSeconds(3 * 60), null, null, null, "track2"),
                RowFactory.create("user3", now.minusSeconds(6 * 60), null, null, null, "track3")

        );
        Dataset<Row> dataFrame = spark.createDataFrame(userTrackRecordsRows,
                schema);
        Encoder<UserTrackRecord> userTrackRecordEncoder = Encoders.bean(UserTrackRecord.class);
        Dataset<UserTrackRecord> userTrackRecordDataset = dataFrame.as(userTrackRecordEncoder);
        userTrackRecordDataset.show();
        when(mockDataReader.readData()).thenReturn(userTrackRecordDataset);
        Assertions.assertEquals(List.of(createPlayCount("track1", 5), createPlayCount("track2", 2)),
                trendsService.getTrends());
    }

    @NotNull
    private static PlayCount createPlayCount(String trackName, long playedCount) {
        PlayCount playCount = new PlayCount();
        playCount.setTrackName(trackName);
        playCount.setPlayedCount(playedCount);
        return playCount;
    }

    @AfterAll
    static void afterAll() {
        spark.close();
    }
}