package com.mdstech.largefile.bus;

import java.util.concurrent.BlockingQueue;

public class BlockingQueueContainer {

    private final BlockingQueue<String> blockingQueue;
    private boolean noData = false;

    public BlockingQueueContainer(BlockingQueue<String> blockingQueue) {
        this.blockingQueue = blockingQueue;
    }

    public boolean putToQueue(String line) {
        boolean isInserted;
        try {
            this.blockingQueue.put(line);
            isInserted = true;
        } catch (InterruptedException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
        return isInserted;
    }

    public String takeFromQueue() {
        if(noData) {
            return null;
        }

        try {
            String data = this.blockingQueue.take();
            System.out.println(data);
            return data;
        } catch (InterruptedException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    public void setNoData(boolean noData) {
        this.noData = noData;
    }

    public int getQueueSize() {
        return this.blockingQueue.size();
    }
}
