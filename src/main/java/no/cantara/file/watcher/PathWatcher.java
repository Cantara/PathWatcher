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
import java.util.EnumSet;
import java.util.Set;
import java.util.stream.Stream;

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

    /**
     * Controls the delay queue for FileCompletelyCreated event
     */
    public static long DELAY_QUEUE_DELAY_TIME = 5000;

    /**
     * Controls the number of retries for FileCompletelyCreated event
     */
    public static int DELAY_QUEUE_RETRY_NUMBER = 3;

    private boolean scanForExistingFilesAtStartup = false;

    private final EventBus fileEventBus;

    private FileWorkerMap fileWorkerMap;

    private Path watchDir;

    private EventWorker eventWorker;

    private FileProducerWorker fileProducerWorker;

    private FileConsumerWorker fileConsumerWorker;

    private Set<FileWatchHandler> createHandler = Sets.newConcurrentHashSet();

    private Set<FileWatchHandler> modifyHandler = Sets.newConcurrentHashSet();

    private Set<FileWatchHandler> removeHandler = Sets.newConcurrentHashSet();

    private Set<FileWatchHandler> fileCompletelyCreatedHandler = Sets.newConcurrentHashSet();

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

    /**
     * Register a handler for a file creation event
     * @param createdFileAction the handler for the event
     */
    public void registerCreatedHandler(FileWatchHandler createdFileAction) {
        createHandler.add(createdFileAction);
    }

    /**
     * Change behaviour of FileNativeEventProducer to generate create file event for existing files in directory
     * @param scanForExistingFilesAtStartup specify if you want event for existing files on the filesystem
     */
    public void setScanForExistingFilesAtStartup(boolean scanForExistingFilesAtStartup) {
        this.scanForExistingFilesAtStartup = scanForExistingFilesAtStartup;
    }

    public Set<FileWatchHandler> getCreateHandlers() {
        return createHandler;
    }

    /**
     * Register a handler for a modification event
     * @param modifyFileAction the handler for the event
     */
    public void registerModifiedHandler(FileWatchHandler modifyFileAction) {
        modifyHandler.add(modifyFileAction);
    }

    public Set<FileWatchHandler> getModifyHandlers() {
        return modifyHandler;
    }

    /**
     * Register a handler for a remove file or directory event.
     * For native file systems wou will receive delete event for directory as well as files.
     * @param removeFileAction the handler for the event
     */
    public void registerRemovedHandler(FileWatchHandler removeFileAction) {
        removeHandler.add(removeFileAction);
    }

    public Set<FileWatchHandler> getRemoveHandlers() {
        return removeHandler;
    }

    /**
     * Register a handler for a event indicating that a file is created and is completely written.
     * This is only supported for native file systems
     * @param fileCompletelyCreatedAction the handler for the event
     */
    public void registerFileCompletelyCreatedHandler(FileWatchHandler fileCompletelyCreatedAction) {
        fileCompletelyCreatedHandler.add(fileCompletelyCreatedAction);
    }

    public Set<FileWatchHandler> getFileCompletelyCreatedHandlers() {
        return fileCompletelyCreatedHandler;
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
        if (event != null) {
            getFileWorkerMap().add(event.getFile(), event);
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
            fileProducerWorker = new FileProducerWorker(pathWatchScannerMode, watchDir, scanForExistingFilesAtStartup);
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
            fileCompletelyCreatedHandler.clear();
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
