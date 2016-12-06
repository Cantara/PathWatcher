package no.cantara.file.watcher;

import no.cantara.file.watcher.support.PathWatchScanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Created by oranheim on 20/10/2016.
 */
public class FileProducerWorker {

    private static Logger log = LoggerFactory.getLogger(FileProducerWorker.class);

    protected static ExecutorService worker;

    private final BlockingQueue producerQueue;

    private final PathWatchScanner mode;

    private final Path dir;

    private final boolean scanForExistingFilesAtStartup;

    private FileEventsProducer fileEventsProducer;

    public FileProducerWorker(PathWatchScanner mode, Path dir) {
        this(mode, dir, false);
    }

    public FileProducerWorker(PathWatchScanner mode, Path dir, boolean scanForExistingFilesAtStartup) {
        worker = Executors.newCachedThreadPool();
        this.producerQueue = new ArrayBlockingQueue(1000);
        this.dir = dir;
        this.mode = mode;
        this.scanForExistingFilesAtStartup = scanForExistingFilesAtStartup;
    }

    public BlockingQueue getQueue() {
        return producerQueue;
    }

    public void start() {
        try {
            log.debug("[start] worker thread");
            if (PathWatchScanner.NATIVE_FILE_SYSTEM.equals(mode)) {
                fileEventsProducer = new FileNativeEventsProducer(producerQueue, dir, scanForExistingFilesAtStartup);
                worker.execute(fileEventsProducer);
            } else if (PathWatchScanner.POLL_FILE_SYSTEM.equals(mode)) {
                fileEventsProducer = new FilePollEventsProducer(producerQueue, dir);
                worker.execute(fileEventsProducer);
            } else {
                throw new UnsupportedOperationException("Unknown FileSystem");
            }
            log.debug("[end] dispatched events.");
        } catch (Exception e) {
            log.error("event failed.", e);
        }
    }

    public boolean isRunning() {
        return !worker.isShutdown();
    }

    public boolean isTerminated() {
        return worker.isTerminated();
    }

    public void shutdown() {
        worker.shutdown();
        try {
            if (!worker.awaitTermination(PathWatcher.WORKER_SHUTDOWN_TIMEOUT, TimeUnit.MILLISECONDS)) {
                worker.shutdownNow();
            }
            log.info("shutdown success");
        } catch (InterruptedException e) {
            log.error("shutdown failed", e);
        }
        if (fileEventsProducer != null) {
            fileEventsProducer.shutdown();
        }
    }
}
