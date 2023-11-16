package com.coralocean.holder;

public interface BlockingReader {
    ExportType chunkType();

    default DataChunk beforeNext() { return null ; }

    default DataChunk afterNext() { return null ; }
    DataChunk next(Long nextOffset);
    int chunks();
    default boolean hasNext(Long nextOffset) {
        return nextOffset == null ||  nextOffset > -1;
    }
}
