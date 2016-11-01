package no.cantara.file.watcher;

import no.cantara.file.watcher.event.FileWatchEvent;
import no.cantara.file.watcher.support.FileWatchKey;
import no.cantara.file.watcher.support.FileWatchState;
import no.cantara.file.watcher.support.RemovedNativeEventBasicFileAttributes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

import static java.nio.file.LinkOption.NOFOLLOW_LINKS;
import static java.nio.file.StandardWatchEventKinds.*;

/**
 * Created by oranheim on 19/10/2016.
 */
public class FileNativeEventsProducer implements Runnable {

    private final static Logger log = LoggerFactory.getLogger(FileNativeEventsProducer.class);

    private final BlockingQueue queue;
    private final Path dir;

    private final WatchService watcher;
    private final Map<WatchKey, Path> keys;
    private final boolean recursive;
    private boolean trace = false;

    public FileNativeEventsProducer(BlockingQueue queue, Path dir) throws IOException {
        this.queue = queue;
        this.dir = dir;

        this.watcher = FileSystems.getDefault().newWatchService();
        this.keys = new HashMap<WatchKey, Path>();
        this.recursive = false;

        if (recursive) {
            log.trace("Native FileSystem Scanning is enabled for: {}", dir);
            registerAll(this.dir);
            log.trace("Done.");
        } else {
            log.trace("Native FileSystem Scanning is enabled for: {}", dir);
            register(this.dir);
        }

        // enable trace after initial registration
        this.trace = true;
    }


    @SuppressWarnings("unchecked")
    static <T> WatchEvent<T> cast(WatchEvent<?> event) {
        return (WatchEvent<T>) event;
    }

    /**
     * Register the given directory with the WatchService
     */
    private void register(Path dir) throws IOException {
        WatchKey key = dir.register(watcher, ENTRY_CREATE, ENTRY_DELETE, ENTRY_MODIFY);
        if (trace) {
            Path prev = keys.get(key);
            if (prev == null) {
                log.trace("register: {}", dir);
            } else {
                if (!dir.equals(prev)) {
                    log.trace("update: {} -> {}", prev, dir);
                }
            }
        }
        keys.put(key, dir);
    }

    /**
     * Register the given directory, and all its sub-directories, with the
     * WatchService.
     */
    private void registerAll(final Path start) throws IOException {
        // register directory and sub-directories
        Files.walkFileTree(start, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs)
                    throws IOException {
                register(dir);
                return FileVisitResult.CONTINUE;
            }
        });
    }

    @Override
    public void run() {
        for (; ; ) {
            try {

                // wait for key to be signalled
                WatchKey key;
                try {
                    key = watcher.take();
                } catch (InterruptedException x) {
                    return;
                }

                Path dir = keys.get(key);
                if (dir == null) {
                    log.error("WatchKey not recognized!!");
                    continue;
                }

                for (WatchEvent<?> event : key.pollEvents()) {
                    WatchEvent.Kind kind = event.kind();

                    // TBD - provide example of how OVERFLOW event is handled
                    if (kind == OVERFLOW) {
                        continue;
                    }

                    // Context for directory entry event is the file name of entry
                    WatchEvent<Path> ev = cast(event);
                    Path name = ev.context();
                    Path child = dir.resolve(name);

                    // print out event
                    //log.trace("{}: {}", event.kind().name(), child);

                    // Event Producer Handling here
                    WatchEvent.Kind eventKind = event.kind();
                    Path eventFile = child;
                    BasicFileAttributes eventAttrs = null;

                    if (Files.exists(child)) {
                        eventAttrs = Files.readAttributes(eventFile, BasicFileAttributes.class);
                    }

                    if (kind == ENTRY_CREATE) {

                        if (!PathWatcher.getInstance().getFileWorkerMap().checkState(eventFile, FileWatchState.DISOCVERED)) {

                            // add file to event
                            FileWatchEvent fileWatchEvent = new FileWatchEvent(eventFile, FileWatchKey.FILE_CREATED, FileWatchState.DISOCVERED, eventAttrs);
                            PathWatcher.getInstance().post(fileWatchEvent);
                            queue.put(fileWatchEvent);
                            log.trace("Discovery - Produced: [{}]{}", fileWatchEvent.getFileWatchKey(), eventFile);
                        }

                    } else if (kind == ENTRY_MODIFY) {

                        if (!PathWatcher.getInstance().getFileWorkerMap().checkState(eventFile, FileWatchKey.FILE_MODIFY)) {
                            FileWatchEvent fileWatchEvent = PathWatcher.getInstance().getFileWorkerMap().getFile(eventFile);
                            BasicFileAttributes newAttrs = Files.readAttributes(eventFile, BasicFileAttributes.class);
                            // we have a modiefied state
                            if (!eventAttrs.equals(newAttrs)) {
                                log.trace("-----------------------> eventFile={}, fileWatchEvent={}, newAttrs={}", eventFile, fileWatchEvent, newAttrs);
                                FileWatchEvent newFileWatchEvent = new FileWatchEvent(eventFile, FileWatchKey.FILE_MODIFY, fileWatchEvent.getFileWatchState(), newAttrs); //  verify that we don't return discovered. use the latest state in map
                                PathWatcher.getInstance().post(newFileWatchEvent);
                                queue.put(newFileWatchEvent);
                                log.trace("Discovery - Produced: [{}]{}", newFileWatchEvent.getFileWatchKey(), eventFile);
                            } else {
                                log.trace("Discovery - Skipping an already scheduled file: {}", eventFile);
                            }
                        }

                    } else if (kind == ENTRY_DELETE) {

                        if (!PathWatcher.getInstance().getFileWorkerMap().checkState(eventFile, FileWatchKey.FILE_REMOVED)) {

                            FileWatchEvent fileWatchEvent = PathWatcher.getInstance().getFileWorkerMap().getFile(eventFile);
                            // todo: check if this is ever called and that only else is hit
                            if (fileWatchEvent != null && event != null) {
                                FileWatchEvent newFileWatchEvent = new FileWatchEvent(eventFile, FileWatchKey.FILE_REMOVED, fileWatchEvent.getFileWatchState(), fileWatchEvent.getAttrs()); //  verify that we don't return discovered. use the latest state in map
                                PathWatcher.getInstance().post(newFileWatchEvent);
                                queue.put(newFileWatchEvent);
                                log.trace("Discovery - Produced: [{}]{}", newFileWatchEvent.getFileWatchKey(), eventFile);

                            // fileWatchEvent and event may be null in case it was deleted in the mean time
                            // todo: check the timing of consumer remove
                            } else if (fileWatchEvent == null && event != null) {
                                FileWatchEvent newFileWatchEvent = new FileWatchEvent(eventFile, FileWatchKey.FILE_REMOVED, FileWatchState.COMPLETED, new RemovedNativeEventBasicFileAttributes()); //  verify that we don't return discovered. use the latest state in map
                                PathWatcher.getInstance().post(newFileWatchEvent);
                                queue.put(newFileWatchEvent);
                                log.trace("Discovery - Produced: [{}]{}", newFileWatchEvent.getFileWatchKey(), eventFile);
                            }
                        }

                    } else {
                        throw new UnsupportedOperationException("Native FS is not implemented for kind=" + kind);
                    }

                    // if directory is created, and watching recursively, then
                    // register it and its sub-directories
                    if (recursive && (kind == ENTRY_CREATE)) {
                        try {
                            if (Files.isDirectory(child, NOFOLLOW_LINKS)) {
                                registerAll(child);
                            }
                        } catch (IOException x) {
                            // ignore to keep sample readbale
                        }
                    }

                }

                // reset key and remove from set if directory no longer accessible
                boolean valid = key.reset();
                if (!valid) {
                    keys.remove(key);

                    // all directories are inaccessible
                    if (keys.isEmpty()) {
                        break;
                    }
                }
            } catch (InterruptedException e) {

            } catch (IOException e) {
                e.printStackTrace();
            }

        }
    }
}
