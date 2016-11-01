package no.cantara.file.watcher;

import com.google.common.collect.ImmutableList;
import no.cantara.file.watcher.event.FileWatchEvent;
import no.cantara.file.watcher.event.PathWatchInternalEvent;
import no.cantara.file.watcher.support.FileWatchKey;
import no.cantara.file.watcher.support.FileWatchState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Created by oranheim on 19/10/2016.
 */
public class FilePollEventsProducer implements Runnable {

    private final static Logger log = LoggerFactory.getLogger(FilePollEventsProducer.class);

    private final BlockingQueue queue;
    private final Path dir;

    public FilePollEventsProducer(BlockingQueue queue, Path dir) {
        this.queue = queue;
        this.dir = dir;
    }

    @Override
    public void run() {
        boolean hasRunOnce = false;
        for( ; ; ) {
            try {
                //log.trace("-> scan directory: {}", dir);
                Runnable r = this;
                Map<Path, BasicFileAttributes> discoveredFiles = new HashMap<>();
                Files.walkFileTree(dir, new SimpleFileVisitor<Path>() {
                    @Override
                    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                        if (!attrs.isDirectory()) {
                            discoveredFiles.put(file, attrs);
                        }
                        return FileVisitResult.CONTINUE;
                    }
                });

                // here compare the new and hold map. If missing elements, we have a removal.
                //log.trace("hasRunOnce: {}", hasRunOnce);
                if (hasRunOnce) {
                    ImmutableList<Path> discoveredFileSet = PathWatcher.getInstance().getFileWorkerMap().keySet();
                    Iterator<Path> it = discoveredFiles.keySet().iterator();

                    //log.trace("Size: {} hasNext={}", discoveredFileSet.size(), discoveredFileSet.get(0));
                    for(int n = 0; n<discoveredFileSet.size(); n++) {
                        Path file = discoveredFileSet.get(n);
                        //log.trace("Compare '{}' with '{}'", file, discoveredFiles.get(file));
                        if (discoveredFiles.keySet().contains(file)) {
                            //log.trace("Should not be removed file: {}", file);
                            continue;
                        } else {
                            // we have a removed item
                            //log.trace("Item [{}]{} was removed!", FileWatchKey.FILE_REMOVED, file);

                            if (!PathWatcher.getInstance().getFileWorkerMap().checkState(file, FileWatchKey.FILE_REMOVED)) {
                                FileWatchEvent event = PathWatcher.getInstance().getFileWorkerMap().getFile(file);
                                if (event != null) {
                                    FileWatchEvent fileWatchEvent = new FileWatchEvent(file, FileWatchKey.FILE_REMOVED, event.getFileWatchState(), event.getAttrs()); //  verify that we don't return discovered. use the latest state in map
                                    PathWatcher.getInstance().post(fileWatchEvent);
                                    queue.put(fileWatchEvent);
                                    log.trace("Discovery - Produced: [{}]{}", fileWatchEvent.getFileWatchKey(), file);
                                }
                            }
                        }
                    }
                }


                discoveredFiles.entrySet().forEach(entry -> {
                    try {
                        Path file = entry.getKey();
                        BasicFileAttributes attrs = entry.getValue();
                        Path filename = file.getFileName();
                        FileTime creationTime = attrs.creationTime();

                        // check if file is already discovered
                        if (!PathWatcher.getInstance().getFileWorkerMap().checkState(file, FileWatchState.DISOCVERED)) {

                            // add file to event
                            FileWatchEvent fileWatchEvent = new FileWatchEvent(file, FileWatchKey.FILE_CREATED, FileWatchState.DISOCVERED, attrs);
                            PathWatcher.getInstance().post(fileWatchEvent);
                            queue.put(fileWatchEvent);
                            log.trace("Discovery - Produced: [{}]{}", fileWatchEvent.getFileWatchKey(), file);
                        } else {

                            if (!PathWatcher.getInstance().getFileWorkerMap().checkState(file, FileWatchKey.FILE_MODIFY)) {
                                FileWatchEvent event = PathWatcher.getInstance().getFileWorkerMap().getFile(file);
                                BasicFileAttributes newAttrs = Files.readAttributes(file, BasicFileAttributes.class);
                                // we have a modiefied state
                                if (!attrs.equals(newAttrs)) {
                                    FileWatchEvent fileWatchEvent = new FileWatchEvent(file, FileWatchKey.FILE_MODIFY, event.getFileWatchState(), newAttrs); //  verify that we don't return discovered. use the latest state in map
                                    PathWatcher.getInstance().post(fileWatchEvent);
                                    queue.put(fileWatchEvent);
                                    log.trace("Discovery - Produced: [{}]{}", fileWatchEvent.getFileWatchKey(), file);
                                } else {
                                    log.trace("Discovery - Skipping an already scheduled file: {}", file);
                                }
                            }
                        }

                        /*
                        if (!Thread.currentThread().isInterrupted())
                            //log.trace("Produced: {}", file);
                            PathWatcher.getInstance().post(new PathWatchInternalEvent(r, String.format("Produced: %s", file.toString())));
                        */
                    } catch (InterruptedException e) {
                        //log.error("FileProducer: {}", e);
                        //throw new RuntimeException(e);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                });

                hasRunOnce = true;
                if (PathWatcher.SCAN_DIRECTORY_INTERVAL == -1) break;
                TimeUnit.MILLISECONDS.sleep(PathWatcher.SCAN_DIRECTORY_INTERVAL);
            } catch (IOException e) {
                log.error("FileProducer: {}", e);
                //throw new RuntimeException(e);
            } catch (InterruptedException e) {
                //log.error("FileProducer: {}", e);
                //throw new RuntimeException(e);
                PathWatcher.getInstance().post(new PathWatchInternalEvent(this, "FileProducer got interrupted"));
                log.trace("FileProducer has ended!");
            }
        }
    }
}
