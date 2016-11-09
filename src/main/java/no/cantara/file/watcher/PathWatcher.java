package no.cantara.file.watcher;

import com.google.common.collect.Sets;
import com.google.common.eventbus.DeadEvent;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import no.cantara.file.watcher.event.FileWatchEvent;
import no.cantara.file.watcher.event.PathWatchInternalEvent;
import no.cantara.file.watcher.support.FileCompletionWorkerMode;
import no.cantara.file.watcher.support.FileWatchHandler;
import no.cantara.file.watcher.support.FileWatchKey;
import no.cantara.file.watcher.support.FileWorkerMap;
import no.cantara.file.watcher.support.PathWatchScanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.Set;

/**
 * Created by oranheim on 19/10/2016.
 * <p>
 * Todo:
 * (done) 1. Make FileWatchKey for CREATED, MODIFIED, REMOVED
 * (done) 2. Make FileWatchState for PENDING, OPEN, LOCKED, CLOSED
 * (done - externalized) 3. Make Callable handler in workers: ReadFileCallable, WriteFileCallable, CopyCallable, MoveCallable, etc.
 * (done) 4. Implemented JDK/OS detector and support WatchService for Async WatchEvent on Zulu JDK8 for linux, so it won't need any poll, but use take
 * (done) 5. Compare changes in file system and remove entries from map
 * <p>
 * Advanced:
 * (tbd) 1. Support multiple watchDirs in workers through use of the worker pool in fileproducer and fileconsumer.
 */
public class PathWatcher {

    private final static Logger log = LoggerFactory.getLogger(PathWatcher.class);

    private static PathWatcher instance;

    public static long SCAN_DIRECTORY_INTERVAL = 5000; // -1 = scan only once

    public static long POLL_INTERVAL = 100;

    public static FileCompletionWorkerMode FILE_COMPLETION_MODE = FileCompletionWorkerMode.TIMEOUT;

    public static long FILE_COMPLETION_TIMEOUT_OR_RETRIES = 2500; // should be split in two

    public static long WORKER_SHUTDOWN_TIMEOUT = 150; // used in force shutdownNow hook

    private final EventBus fileEventBus;

    private FileWorkerMap fileWorkerMap;

    private Path watchDir;

    private EventWorker eventWorker;

    private FileProducerWorker fileProducerWorker;

    private FileConsumerWorker fileConsumerWorker;

    private Set<FileWatchHandler> createHandler = Sets.newConcurrentHashSet();

    private Set<FileWatchHandler> modifyHandler = Sets.newConcurrentHashSet();

    private Set<FileWatchHandler> removeHandler = Sets.newConcurrentHashSet();

    private PathWatchScanner pathWatchScannerMode;

    private boolean running;

    private PathWatcher() {
        if (FileSystemSupport.isLinuxFileSystem()) {
            pathWatchScannerMode = PathWatchScanner.NATIVE_FILE_SYSTEM;
        } else if (FileSystemSupport.isMacOSFileSystem()) {
            pathWatchScannerMode = PathWatchScanner.POLL_FILE_SYSTEM;
        } else if (FileSystemSupport.isWindowsFileSystem()) {
            pathWatchScannerMode = PathWatchScanner.NATIVE_FILE_SYSTEM;
        } else {
            throw new UnsupportedOperationException("Unknown FileSystem");
        }
        fileEventBus = new EventBus();
        subscribe(new DeadEventsSubscriber());
        subscribe(this);
    }

    public void registerCreatedHandler(FileWatchHandler createdFileAction) {
        createHandler.add(createdFileAction);
    }

    public Set<FileWatchHandler> getCreateHandlers() {
        return createHandler;
    }

    public void registerModifiedHandler(FileWatchHandler modifyFileAction) {
        modifyHandler.add(modifyFileAction);
    }

    public Set<FileWatchHandler> getModifyHandlers() {
        return modifyHandler;
    }

    public void registerRemovedHandler(FileWatchHandler removeFileAction) {
        removeHandler.add(removeFileAction);
    }

    public Set<FileWatchHandler> getRemoveHandlers() {
        return removeHandler;
    }

    public void forceFileSystemScannerMode(PathWatchScanner mode) {
        pathWatchScannerMode = mode;
    }

    public boolean isNativeEvents() {
        return PathWatchScanner.NATIVE_FILE_SYSTEM.equals(pathWatchScannerMode);
    }

    public boolean isPollEvents() {
        return PathWatchScanner.POLL_FILE_SYSTEM.equals(pathWatchScannerMode);
    }

    public static class DeadEventsSubscriber {
        @Subscribe
        public void handleDeadEvent(DeadEvent deadEvent) {
            log.error("DEAD EVENT: {}", deadEvent.getEvent());
        }
    }

    @Subscribe
    private synchronized void eventHandler(PathWatchInternalEvent event) {
        try {
            eventWorker.getQueue().put(event);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Subscribe
    private synchronized void fileWatchEvent(FileWatchEvent event) {
        if (FileWatchKey.FILE_CREATED.equals(event.getFileWatchKey())) {
            getFileWorkerMap().add(event.getFile(), event);
            //log.trace("Added Created to FileWorkerMap: {}", event.toString());
        } else if (FileWatchKey.FILE_MODIFY.equals(event.getFileWatchKey())) {
            getFileWorkerMap().add(event.getFile(), event);
            //log.trace("Added Modified to FileWorkerMap: {}", event.toString());
        } else if (FileWatchKey.FILE_REMOVED.equals(event.getFileWatchKey())) {
            getFileWorkerMap().add(event.getFile(), event);
            //log.trace("Added Removed from FileWorkerMap: {}", event.toString());
        }
    }

    public synchronized FileWorkerMap getFileWorkerMap() {
        if (fileWorkerMap == null) {
            fileWorkerMap = new FileWorkerMap();
        }
        return fileWorkerMap;
    }

    public void setPathScanInterval(long timeout) {
        SCAN_DIRECTORY_INTERVAL = timeout;
    }

    public void setWorkerMode(FileCompletionWorkerMode workerMode) {
        FILE_COMPLETION_MODE = workerMode;
    }

    public void setTimeoutOrRetryInterval(int timeoutOrRetryInterval) {
        FILE_COMPLETION_TIMEOUT_OR_RETRIES = timeoutOrRetryInterval;
    }

    public void setThreadPollInterval(int threadPollInterval) {
        POLL_INTERVAL = threadPollInterval;
    }

    public void setWorkerShutdownTimeout(long workerShutdownTimeout) {
        WORKER_SHUTDOWN_TIMEOUT = workerShutdownTimeout;
    }

    protected void subscribe(Object object) {
        fileEventBus.register(object);
    }

    protected void post(PathWatchInternalEvent event) {
        fileEventBus.post(event);
    }

    protected void post(FileWatchEvent event) {
        fileEventBus.post(event);
    }

    public boolean isRunning() {
        return ((fileProducerWorker != null || fileConsumerWorker != null) ? (fileProducerWorker.isRunning() || fileConsumerWorker.isRunning()) : false);
    }

    public boolean isInterrupted() {
        return ((fileProducerWorker != null || fileConsumerWorker != null) ? (fileProducerWorker.isTerminated() || fileConsumerWorker.isTerminated()) : false);
    }

    public void watch(Path directory) {
        log.trace("Watch directory: {}", directory);
        watchDir = directory;
    }

    public String getConfigInfo() {
        JsonObject json = new JsonObject();
        json.addProperty("pathWatchScannerMode", pathWatchScannerMode.toString());
        json.addProperty("watchDir", watchDir.toString());
        if (isPollEvents()) {
            json.addProperty("workerMode", FILE_COMPLETION_MODE.toString());
            json.addProperty("scanInterval", SCAN_DIRECTORY_INTERVAL);
            json.addProperty("timeoutOrRetryInterval", FILE_COMPLETION_TIMEOUT_OR_RETRIES);
            json.addProperty("threadPollInterval", POLL_INTERVAL);
        }
        json.addProperty("workerShutdownTimeout", WORKER_SHUTDOWN_TIMEOUT);

        Gson gson = new GsonBuilder().setPrettyPrinting().disableHtmlEscaping().create();
        return gson.toJson(json);
    }

    public void start() {
        if (!isRunning()) {
            eventWorker = new EventWorker();
            fileProducerWorker = new FileProducerWorker(pathWatchScannerMode, watchDir);
            fileConsumerWorker = new FileConsumerWorker(pathWatchScannerMode, fileProducerWorker.getQueue(), watchDir);

            eventWorker.start();
            fileConsumerWorker.start();
            fileProducerWorker.start();

            running = true;
            log.trace("PathWatcher is started with configuration:\n{}", getConfigInfo());
        } else {
            log.trace("Cannot start PathWatcher because it is already running!");
        }
    }

    public void stop() {
        if (isRunning()) {
            fileProducerWorker.shutdown();
            fileConsumerWorker.shutdown();
            eventWorker.shutdown();
            createHandler.clear();
            modifyHandler.clear();
            removeHandler.clear();
            getFileWorkerMap().clear();
            fileWorkerMap = null;
            running = false;
            log.trace("PathWatcher is now shutdown!");
        } else {
            log.trace("Cannot stop PathWatcher because it is shutdown!");
        }
    }

    public static PathWatcher getInstance() {
        if (instance == null) {
            instance = new PathWatcher();
        }
        return instance;
    }
}
