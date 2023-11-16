package com.coralocean.holder;

public interface ExportType {

    String getExportType();
    String getExportName();
    default String getExportTypeDescription() { return null ;}
    default String getExportSuffix() {return ".csv";}
}
