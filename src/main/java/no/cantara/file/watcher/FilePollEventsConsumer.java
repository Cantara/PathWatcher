package no.cantara.file.watcher;

import no.cantara.file.watcher.event.FileWatchEvent;
import no.cantara.file.watcher.event.PathWatchInternalEvent;
import no.cantara.file.watcher.support.FileWatchHandler;
import no.cantara.file.watcher.support.FileWatchKey;
import no.cantara.file.watcher.support.FileWatchState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.RejectedExecutionException;

/**
 * Created by oranheim on 19/10/2016.
 */
public class FilePollEventsConsumer implements Runnable {

    private final static Logger log = LoggerFactory.getLogger(FilePollEventsConsumer.class);

    private final FileDetermineCompletionWorker fileDetermineCompletionWorker;

    private final BlockingQueue queue;

    private final Path dir;

    public FilePollEventsConsumer(FileDetermineCompletionWorker fileDetermineCompletionWorker, BlockingQueue queue, Path dir) {
        this.fileDetermineCompletionWorker = fileDetermineCompletionWorker;
        this.queue = queue;
        this.dir = dir;
    }

    @Override
    public void run() {
        for (; ; ) {
            try {
                FileWatchEvent event = (FileWatchEvent) queue.take();
                log.trace("Consumed: [{}]{} and check if it is available", event.getFileWatchKey(), event);

                if (FileWatchKey.FILE_CREATED.equals(event.getFileWatchKey()) || FileWatchKey.FILE_COMPLETELY_CREATED.equals(event.getFileWatchKey())) {
                    try {
                        fileDetermineCompletionWorker
                                .execute(event.getFile(), PathWatcher.POLL_INTERVAL, PathWatcher.FILE_COMPLETION_MODE, PathWatcher.FILE_COMPLETION_TIMEOUT_OR_RETRIES);
                    } catch (RejectedExecutionException e) {
                        // todo: at this point this error occurs when the PathWatcher is shutdown. do nothing
                        break;
                    }
                    while (!fileDetermineCompletionWorker.isDone() && !fileDetermineCompletionWorker.isInterrupted()) {
                        Thread.sleep(PathWatcher.POLL_INTERVAL);
                    }

                    // are we ready to call an action
                    if (fileDetermineCompletionWorker.isDone() && !fileDetermineCompletionWorker.isInterrupted()) {
                        BasicFileAttributes attrs = Files.readAttributes(event.getFile(), BasicFileAttributes.class);
                        FileWatchEvent fileWatchEvent = new FileWatchEvent(event.getFile(), event.getFileWatchKey(), FileWatchState.COMPLETED, attrs);
                        PathWatcher.getInstance().post(fileWatchEvent);

                        Set<FileWatchHandler> actions = FileWatchKey.FILE_CREATED.equals(event.getFileWatchKey())
                                ? PathWatcher.getInstance().getCreateHandlers()
                                : PathWatcher.getInstance().getFileCompletelyCreatedHandlers();

                        invokeHandlers(fileWatchEvent, actions);
                    } else {
                        // todo: dead letter handling here
                        BasicFileAttributes attrs;
                        if (Files.exists(event.getFile())) {
                            attrs = Files.readAttributes(event.getFile(), BasicFileAttributes.class);
                        } else {
                            attrs = event.getAttrs();
                        }
                        FileWatchEvent fileWatchEvent = new FileWatchEvent(event.getFile(), event.getFileWatchKey(), FileWatchState.INCOMPLETE, attrs);
                        PathWatcher.getInstance().post(fileWatchEvent);
                    }
                } else if (FileWatchKey.FILE_MODIFY.equals(event.getFileWatchKey())) {
                    PathWatcher.getInstance().post(event);
                    Set<FileWatchHandler> actions = PathWatcher.getInstance().getModifyHandlers();
                    invokeHandlers(event, actions);
                } else if (FileWatchKey.FILE_REMOVED.equals(event.getFileWatchKey())) {
                    PathWatcher.getInstance().post(event);
                    Set<FileWatchHandler> actions = PathWatcher.getInstance().getRemoveHandlers();
                    invokeHandlers(event, actions);
                    PathWatcher.getInstance().getFileWorkerMap().remove(event.getFile());
                }
            } catch (InterruptedException e) {
                //e.printStackTrace();
                PathWatcher.getInstance().post(new PathWatchInternalEvent(this, "FileConsumer got interrupted"));
                log.trace("FileConsumer has ended! {}", e);
            } catch (IOException e) {
                //e.printStackTrace();
                log.error("FileConsumer failed: \n{}", e);
            }
        }
    }

    private void invokeHandlers(FileWatchEvent fileWatchEvent, Set<FileWatchHandler> actions) {
        for (FileWatchHandler action : actions) {
            try {
                action.invoke(fileWatchEvent);
            } catch (Exception e) {
                log.error("Handler exception from event type {}:\n{}", fileWatchEvent.getFileWatchKey().toString(), e);
            }
        }
    }
}
