package com.coralocean.holder;


import lombok.Data;

import java.util.List;

@Data
public class DataChunk {
    private List<?> resultSet;
    private String saveFileName;
    private long nextOffset;

    public DataChunk(String saveName, int chunkSize, List<? extends Chunkable> resultSet) {
        this.saveFileName = saveName;
        this.resultSet = resultSet;
        this.nextOffset = resultSet.size() < chunkSize ? -1 : resultSet.get( resultSet.size() - 1).chunkId();
    }

    public DataChunk(String saveName, List<List<String>> resultSet) {
        this.saveFileName = saveName;
        this.resultSet = resultSet;
        this.nextOffset = resultSet.size();
    }


    public boolean isLast() {
        return nextOffset == -1;
    }
}
