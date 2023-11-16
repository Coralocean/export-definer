package com.coralocean.dispather;

/**
 * 调度器
 */
public interface ExportDispatcher {
    <T> void dispatch(DispatchHandler<T> dispatchHandler);
    <T> boolean applyDispatch(T reqData);

    <T> void releaseDispatch();
}
