package no.cantara.file.watcher;


import no.cantara.file.util.CommonUtil;
import no.cantara.file.watcher.event.FileWatchEvent;
import no.cantara.file.watcher.support.FileWatchKey;
import no.cantara.file.watcher.support.FileWatchState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.concurrent.BlockingQueue;

/**
 * Checks if a file is completely written. If it is, the event is sent to the regular FileWatchEvent queue.
 * If the file is not completely written yet it is sent to a delay queue, and retried after a configurable delay.
 */
public class DelayedFileCompletelyCreatedProducer {
    private static final Logger logger = LoggerFactory.getLogger(DelayedFileCompletelyCreatedProducer.class);
    private BlockingQueue<DelayedFileWatchEvent> delayQueue;
    private BlockingQueue<FileWatchEvent> completeEventsQueue;

    public DelayedFileCompletelyCreatedProducer(BlockingQueue<DelayedFileWatchEvent> delayQueue, BlockingQueue<FileWatchEvent> completeEventsQueue) {
        this.completeEventsQueue = completeEventsQueue;
        this.delayQueue = delayQueue;
    }


    public void createFileCompletelyCreatedEvent(Path eventFile, BasicFileAttributes fileAttributes) throws InterruptedException {
        if (!Files.isDirectory(eventFile)) {
            if (CommonUtil.isFileCompletelyWritten(eventFile.toFile())) {
                // add file to event
                FileWatchEvent fileCompletelyCreatedWatchEvent = new FileWatchEvent(eventFile, FileWatchKey.FILE_COMPLETELY_CREATED, FileWatchState.DISOCVERED, fileAttributes);
                PathWatcher.getInstance().post(fileCompletelyCreatedWatchEvent);
                completeEventsQueue.put(fileCompletelyCreatedWatchEvent);
                logger.trace("Discovery - Produced: [{}]{}", fileCompletelyCreatedWatchEvent.getFileWatchKey(), eventFile);
            } else {
                FileWatchEvent fileCompletelyCreatedWatchEvent = new FileWatchEvent(eventFile, FileWatchKey.FILE_COMPLETELY_CREATED, FileWatchState.INCOMPLETE, fileAttributes);
                DelayedFileWatchEvent delayedFileWatchEvent = new DelayedFileWatchEvent(fileCompletelyCreatedWatchEvent, PathWatcher.DELAY_QUEUE_DELAY_TIME);
                PathWatcher.getInstance().post(fileCompletelyCreatedWatchEvent);
                delayQueue.put(delayedFileWatchEvent);
                logger.trace("Discovery - incomplete file [{}]{}, delay creation with {} ms", fileCompletelyCreatedWatchEvent.getFileWatchKey(), eventFile,
                        PathWatcher.DELAY_QUEUE_DELAY_TIME);
            }
        }
    }
}
