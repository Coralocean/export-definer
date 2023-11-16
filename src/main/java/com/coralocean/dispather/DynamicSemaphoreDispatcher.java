package com.coralocean.dispather;


import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

public class DynamicSemaphoreDispatcher implements ExportDispatcher {
    private final AtomicInteger currentMaxRequests;
    private final int initMaxRequests;
    private volatile Semaphore semaphore;




    public DynamicSemaphoreDispatcher(int initMaxRequests) {
        semaphore = new Semaphore(initMaxRequests);
        this.initMaxRequests = initMaxRequests;
        currentMaxRequests = new AtomicInteger(initMaxRequests);

    }

    @Override
    public <T> void dispatch(DispatchHandler<T> dispatchHandler) {
        dispatchHandler.handleExport(this);
    }

    @Override
    public <T> boolean applyDispatch(T reqData) {
        if (semaphore.availablePermits() <= getBaseRequests()) {
            adjustMaxRequestsBaseOnJvm();
        }
        return semaphore.tryAcquire();
    }

    @Override
    public <T> void releaseDispatch() {
        semaphore.release();
    }

    // 这里应该不需要同步代码块
    private void adjustMaxRequestsBaseOnJvm() {
        Runtime runtime = Runtime.getRuntime();
        long freeMemory = runtime.freeMemory();
        long totalMemory = runtime.totalMemory();
        long usedMemory = totalMemory - freeMemory;
        if (usedMemory > totalMemory * 0.8) {
            // 告警
            int num  = Math.max(initMaxRequests, currentMaxRequests.get() - semaphore.availablePermits() + 1);
            currentMaxRequests.set(num);
            updateSemaphore();
            semaphore.drainPermits();
        } else {
            currentMaxRequests.getAndAdd(5);
            updateSemaphore();
        }
    }

    private void adjustMaxRequestsBaseOnJvmBytes() {
        Runtime runtime = Runtime.getRuntime();
        long freeMemory = runtime.freeMemory();
        if (freeMemory <= 10_000_000L) {
            // 告警
            int num  = Math.max(initMaxRequests, currentMaxRequests.get() - semaphore.availablePermits() + 1);
            currentMaxRequests.set(num);
            updateSemaphore();
            semaphore.drainPermits();
        } else {
            currentMaxRequests.getAndAdd(5);
            updateSemaphore();
        }
    }

    private int getBaseRequests() {
        return (int) (currentMaxRequests.get() * 0.7);
    }


    private synchronized void updateSemaphore() {
        semaphore = new Semaphore(currentMaxRequests.get());
    }

}
