package no.cantara.file.watcher;

import no.cantara.file.util.CommonUtil;
import no.cantara.file.watcher.event.FileWatchEvent;
import no.cantara.file.watcher.support.FileWatchKey;
import no.cantara.file.watcher.support.FileWatchState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.concurrent.BlockingQueue;

/**
 * Consumer of the delayed FileWatchEvents
 * Create a FILE_COMPLETELY_CREATED event if the file is finished within the time limit
 * Time limit is controlled by {@link PathWatcher#DELAY_QUEUE_DELAY_TIME} and  {@link PathWatcher#DELAY_QUEUE_RETRY_NUMBER}
 */
public class DelayedFileCompletelyCreatedConsumer implements Runnable {
    private static Logger logger = LoggerFactory.getLogger(DelayedFileCompletelyCreatedConsumer.class);

    private BlockingQueue<DelayedFileWatchEvent> queue;
    private BlockingQueue<FileWatchEvent> completeEventsQueue;

    public DelayedFileCompletelyCreatedConsumer(BlockingQueue<DelayedFileWatchEvent> queue, BlockingQueue<FileWatchEvent> completeEventsQueue) {
        super();
        this.queue = queue;
        this.completeEventsQueue = completeEventsQueue;
    }

    @Override
    public void run() {
        logger.debug("Start running...");

        while (true) {
            try {
                DelayedFileWatchEvent delayedFileWatchEvent = queue.take();
                logger.debug("Event taken from queue {}", delayedFileWatchEvent.toString());
                Path eventFile = delayedFileWatchEvent.getFileWatchEvent().getFile();
                if (CommonUtil.isFileCompletelyWritten(eventFile.toFile())) {
                    try {
                        FileWatchEvent fileWatchEvent = new FileWatchEvent(eventFile, FileWatchKey.FILE_COMPLETELY_CREATED, FileWatchState.DISOCVERED, Files.readAttributes(eventFile, BasicFileAttributes.class));
                        PathWatcher.getInstance().post(fileWatchEvent);
                        completeEventsQueue.put(fileWatchEvent);
                        logger.trace("Discovery - Produced: [{}]{}", fileWatchEvent.getFileWatchKey(), eventFile);
                    } catch (IOException ioe) {
                        logger.warn("Failed to read FileAttributes for {}, event discarded", eventFile.toString());
                    }
                } else {
                    if (delayedFileWatchEvent.incrementCounter() < PathWatcher.DELAY_QUEUE_RETRY_NUMBER) {
                        queue.put(delayedFileWatchEvent);
                        logger.debug("Still not ready delay the file again");
                    } else {
                        logger.warn("File {} not possible to read after max tries ({})", PathWatcher.DELAY_QUEUE_RETRY_NUMBER);
                    }
                }

            } catch (InterruptedException e) {
               logger.trace("Interrupted");
            }
        }
    }
}

