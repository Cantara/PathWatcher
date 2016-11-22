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
 * Create an FileWatchEvent if file is finished otherwise delay it again
 */
public class DelayedFileWatchEventConsumer implements Runnable {
    private static Logger logger = LoggerFactory.getLogger(DelayedFileWatchEventConsumer.class);

    private BlockingQueue<DelayedFileWatchEvent> queue;
    private BlockingQueue<FileWatchEvent> completeEventsQueue;

    public DelayedFileWatchEventConsumer(BlockingQueue<DelayedFileWatchEvent> queue, BlockingQueue<FileWatchEvent> completeEventsQueue) {
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
                    if (!PathWatcher.getInstance().getFileWorkerMap().checkState(eventFile, FileWatchState.DISOCVERED)) {
                        try {
                            FileWatchEvent fileWatchEvent = new FileWatchEvent(eventFile, FileWatchKey.FILE_CREATED, FileWatchState.DISOCVERED, Files.readAttributes(eventFile, BasicFileAttributes.class));
                            PathWatcher.getInstance().post(fileWatchEvent);
                            completeEventsQueue.put(fileWatchEvent);
                            logger.trace("Discovery - Produced: [{}]{}", fileWatchEvent.getFileWatchKey(), eventFile);
                        } catch (IOException ioe) {
                            logger.warn("Failed to read FileAttributes for {}, event discarded", eventFile.toString());
                        }
                    } else {
                        logger.trace("File {} already handled", eventFile);
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

