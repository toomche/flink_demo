package com.yss.flink.table.entity;

import lombok.Data;

@Data
public class WC {

    private String word;

    private int count;

    public WC(String word, int count) {
        this.word = word;
        this.count = count;
    }
}
