package no.cantara.file.watcher;

import no.cantara.file.watcher.event.FileWatchEvent;

import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

/**
 * The implementation of the Delayed interface used for queueing FileWatchEvents until the file creation is finished.
 */
public class DelayedFileWatchEvent implements Delayed {
    private FileWatchEvent fileWatchEvent;
    private long startTime;
    private int counter = 0;


    public int incrementCounter() {
        return counter++;
    }

    public DelayedFileWatchEvent(FileWatchEvent fileWatchEvent, long delay){
        this.fileWatchEvent = fileWatchEvent;
        this.startTime = System.currentTimeMillis() + delay;
    }

    public FileWatchEvent getFileWatchEvent() {
        return fileWatchEvent;
    }

    @Override
    public long getDelay(TimeUnit unit) {
        long diff = startTime - System.currentTimeMillis();
        return unit.convert(diff, TimeUnit.MILLISECONDS);
    }

    @Override
    public int compareTo(Delayed o) {
        if (this.startTime < ((DelayedFileWatchEvent) o).startTime) {
            return -1;
        }
        if (this.startTime > ((DelayedFileWatchEvent) o).startTime) {
            return 1;
        }
        return 0;
    }

}
