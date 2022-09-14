package com.home.mtrends.model;

import lombok.Data;

import java.io.Serializable;

@Data
public class PlayCount implements Serializable {
    private String trackName;
    private long playedCount;
}
