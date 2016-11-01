package no.cantara.file.watcher;

import no.cantara.file.watcher.support.PathWatchScanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Created by oranheim on 20/10/2016.
 */
public class FileConsumerWorker {

    private static Logger log = LoggerFactory.getLogger(FileConsumerWorker.class);

    protected static ExecutorService worker;
    private FileDetermineCompletionWorker fileDetermineCompletionWorker;
    private final BlockingQueue produerQueue;
    private final Path dir;
    private PathWatchScanner mode;

    public FileConsumerWorker(PathWatchScanner mode, BlockingQueue produerQueue, Path dir) {
        worker = Executors.newCachedThreadPool();
        this.mode = mode;
        this.produerQueue = produerQueue;
        this.dir = dir;
    }

    public void start() {
        try {
            log.debug("[start] worker thread");
            if (PathWatchScanner.NATIVE_FILE_SYSTEM.equals(mode)) {
                worker.execute(new FileNativeEventsConsumer(produerQueue, dir));

            } else if (PathWatchScanner.POLL_FILE_SYSTEM.equals(mode)) {
                if (fileDetermineCompletionWorker == null) {
                    fileDetermineCompletionWorker = new FileDetermineCompletionWorker();
                }
                worker.execute(new FilePollEventsConsumer(fileDetermineCompletionWorker, produerQueue, dir));

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
                if (PathWatchScanner.POLL_FILE_SYSTEM.equals(mode)) {
                    fileDetermineCompletionWorker.shutdown();
                    fileDetermineCompletionWorker = null;
                }
            }
            log.info("shutdown success");
        } catch (InterruptedException e) {
            log.error("shutdown failed",e);
        }
    }


}
