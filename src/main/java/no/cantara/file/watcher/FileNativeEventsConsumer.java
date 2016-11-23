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

/**
 * Created by oranheim on 19/10/2016.
 */
public class FileNativeEventsConsumer implements Runnable {

    private final static Logger log = LoggerFactory.getLogger(FileNativeEventsConsumer.class);

    private final BlockingQueue queue;
    private final Path dir;

    public FileNativeEventsConsumer(BlockingQueue queue, Path dir) {
        this.queue = queue;
        this.dir = dir;
    }

    @Override
    public void run() {
        for( ; ; ) {
            try {
                FileWatchEvent event = (FileWatchEvent) queue.take();
                log.trace("Consumed: [{}]{} and check if it is available", event.getFileWatchKey(), event);

                if (FileWatchKey.FILE_CREATED.equals(event.getFileWatchKey())) {

                    // are we ready to call an action
                    BasicFileAttributes attrs = Files.readAttributes(event.getFile(), BasicFileAttributes.class);
                    FileWatchEvent fileWatchEvent = new FileWatchEvent(event.getFile(), event.getFileWatchKey(), FileWatchState.COMPLETED, attrs);
                    PathWatcher.getInstance().post(fileWatchEvent);

                    Set<FileWatchHandler> actions = PathWatcher.getInstance().getCreateHandlers();
                    for (FileWatchHandler action : actions) {
                        try {
                            action.invoke(fileWatchEvent);
                        } catch (Exception e) {
                            log.error("Created handler exception:\n{}", e);
                        }
                    }

                } else if (FileWatchKey.FILE_MODIFY.equals(event.getFileWatchKey())) {

                    PathWatcher.getInstance().post(event);
                    Set<FileWatchHandler> actions = PathWatcher.getInstance().getModifyHandlers();
                    for (FileWatchHandler action : actions) {
                        try {
                            action.invoke(event);
                        } catch (Exception e) {
                            log.error("Modify handler exception:\n{}", e);
                        }
                    }

                } else if (FileWatchKey.FILE_REMOVED.equals(event.getFileWatchKey())) {

                    PathWatcher.getInstance().post(event);
                    Set<FileWatchHandler> actions = PathWatcher.getInstance().getRemoveHandlers();
                    for (FileWatchHandler action : actions) {
                        try {
                            action.invoke(event);
                        } catch (Exception e) {
                            log.error("Removed handler exception:\n{}", e);
                        }
                    }

                    PathWatcher.getInstance().getFileWorkerMap().remove(event.getFile());

                } else if (FileWatchKey.FILE_COMPLETELY_CREATED.equals(event.getFileWatchKey())) {

                    // are we ready to call an action
                    BasicFileAttributes attrs = Files.readAttributes(event.getFile(), BasicFileAttributes.class);
                    FileWatchEvent fileWatchEvent = new FileWatchEvent(event.getFile(), event.getFileWatchKey(), FileWatchState.COMPLETED, attrs);
                    PathWatcher.getInstance().post(fileWatchEvent);

                    Set<FileWatchHandler> actions = PathWatcher.getInstance().getFileCompletelyCreatedHandlers();
                    for (FileWatchHandler action : actions) {
                        try {
                            action.invoke(fileWatchEvent);
                        } catch (Exception e) {
                            log.error("Created handler exception:\n{}", e);
                        }
                    }

                } else {
                    log.warn("Unhandled fileWatchKey {}", event.getFileWatchKey());
                }

            } catch (InterruptedException e) {
                //e.printStackTrace();
                PathWatcher.getInstance().post(new PathWatchInternalEvent(this, "FileConsumer got interrupted"));
                log.trace("FileConsumer has ended!");
            } catch (IOException e) {
                //e.printStackTrace();
                log.error("FileConsumer failed: \n{}", e);
            }
        }
    }
}
