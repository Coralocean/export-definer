package com.coralocean.dispather;

public interface DispatchHandler<T> {
    public long queryExportRows();
    public void handleExport(ExportDispatcher dispatcher);
}
