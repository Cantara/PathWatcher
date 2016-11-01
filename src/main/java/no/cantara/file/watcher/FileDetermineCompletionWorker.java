package no.cantara.file.watcher;

import no.cantara.file.watcher.support.FileCompletionWorkerMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Created by oranheim on 20/10/2016.
 */
public class FileDetermineCompletionWorker {

    private static Logger log = LoggerFactory.getLogger(FileDetermineCompletionWorker.class);

    protected static ExecutorService worker;
    private DetermineFileCompletionRunnable determineFileCompletionRunnable;
    private Path file;
    private long pollInterval;
    private FileCompletionWorkerMode mode;
    private long retriesOrTimeout;

    public FileDetermineCompletionWorker() {
        worker = Executors.newCachedThreadPool();
    }

    private class DetermineFileCompletionRunnable implements Runnable {

        private boolean done;
        private boolean interrupted;

        private DetermineFileCompletionRunnable() {
        }

        @Override
        public void run() {

            long lastModified = 0;
            long lastFileSize = 0;
            long totalReadFileSize = 0;

            long numberOfPollIterations = 0;
            long totalPollIterationTime = 0;
            long averagePollIteration = 0;
            long lastAveragePollIteration = 0;

            long noChangeCount = 0;
            long retryCount = retriesOrTimeout;

            long retryTime = 0;
            long timeout = retriesOrTimeout;

            boolean dangling = false;

            while(true) {
                try {
                    long take = System.currentTimeMillis();
                    numberOfPollIterations++;

                    if (!Files.exists(file)) {
                        throw new InterruptedException("File does not exist! Probably due to removal of file. Quiting task.");
                    }

                    BasicFileAttributes attr = Files.readAttributes(file, BasicFileAttributes.class);
                    long currentLastModified = attr.lastModifiedTime().toMillis();
                    long currentFileSize = attr.size();
                    totalReadFileSize += (currentFileSize - lastFileSize);

                    boolean content_was_changed = (currentFileSize != lastFileSize && currentLastModified != lastModified);
                    boolean content_was_unchanged = (currentFileSize == lastFileSize && currentLastModified == lastModified);

                    String readStateText = null;
                    if (content_was_changed) {
                        dangling = false;
                        noChangeCount = 0; // only used by !USE_TIMEOUT_INSTEAD_RETRY_COUNT
                        lastAveragePollIteration = 0;
                        retryTime = 0; // only used by USE_TIMEOUT_INSTEAD_RETRY_COUNT
                        readStateText = "data";

                    } else if (content_was_unchanged) {

                        if (FileCompletionWorkerMode.PASSTHROUGH.equals(mode)) {
                            log.trace("Passthrough: Interrupt because it must complete on time!");
                            // do nothing
                            throw new InterruptedException();

                        } else if (dangling && FileCompletionWorkerMode.RETRIES.equals(mode) && (noChangeCount < retryCount)) {
                            // do a retry loop at the end in order to give som threshold before we conclude a success
                            // inc for a retry
                            noChangeCount++;
                            // possible new feature: bookmark the "normal" average, so we have a reference value to compare with (experimental)
                            if (lastAveragePollIteration == 0) lastAveragePollIteration = averagePollIteration;

                            Thread.sleep(pollInterval);

                            take = System.currentTimeMillis() - take;
                            totalPollIterationTime += take;
                            averagePollIteration = totalPollIterationTime / numberOfPollIterations;
                            log.trace("Poll threshold retries by {}/{}, fileChange: {}/{}, millisChange: {} with avgExecTime: {}ms vs lastExecTime: {}ms", noChangeCount, retryCount, (currentFileSize - lastFileSize), totalReadFileSize, (currentLastModified - lastModified), averagePollIteration, lastAveragePollIteration);

                            continue;

                        } else if (dangling && FileCompletionWorkerMode.TIMEOUT.equals(mode) && (retryTime < timeout)) {
                            if (lastAveragePollIteration == 0) lastAveragePollIteration = averagePollIteration;

                            Thread.sleep(pollInterval);

                            take = System.currentTimeMillis() - take;
                            retryTime += take;
                            totalPollIterationTime += take;
                            averagePollIteration = totalPollIterationTime / numberOfPollIterations;
                            log.trace("Poll threshold timeout by {}/{}ms, fileChange: {}/{}, millisChange: {} with avgExecTime: {}ms vs lastExecTime: {}ms", retryTime, timeout, (currentFileSize - lastFileSize), totalReadFileSize, (currentLastModified - lastModified), averagePollIteration, lastAveragePollIteration);

                            continue;

                        } else /* we got data of unknown state */ {
                            readStateText = "(SEVERE UNKNOWN STATE)";
                        }

                        log.trace("Received data completed successfully: {}", file.toString());

                        break; //return true;

                    } else /* we got dangling data, meaning the stream came to a temporary halt */ {
                        readStateText = "dangling data";
                        dangling = true; /* todo: this requires some more exploration to optimize trigger for poll threshold. It may be possible to control the threshold poll in a more better way. debug debug.. */
                    }

                    log.trace("Received {}: {}, fileChange: {}/{}, lastModifiedChangeMillis: {}, lastModified: {}", readStateText, file.getFileName(), (currentFileSize - lastFileSize), totalReadFileSize, (numberOfPollIterations == 1 ? 0 : (currentLastModified - lastModified)), currentLastModified);

                    lastFileSize = currentFileSize;
                    lastModified = currentLastModified;

                    Thread.sleep(pollInterval);

                    take = System.currentTimeMillis() - take;
                    totalPollIterationTime += take;
                    averagePollIteration = totalPollIterationTime / numberOfPollIterations;

                } catch (FileNotFoundException e) {
                    e.printStackTrace();
                    done = false;
                    return;
                } catch (IOException e) {
                    e.printStackTrace();
                    done = false;
                    return;
                } catch (InterruptedException e) {
                    //e.printStackTrace();
                    interrupted = true;
                    done = false;
                    return;
                }
            }
            done = true;
        }

        public boolean isDone() {
            return done;
        }

        public boolean isInterrupted() {
            return interrupted;
        }
    }

    private void start() {
        try {
            log.debug("[start] worker thread");
            determineFileCompletionRunnable = new DetermineFileCompletionRunnable();
            worker.execute(determineFileCompletionRunnable);
            log.debug("[end] dispatched events.");
        } catch (Exception e) {
            log.error("event failed.", e);
        }
    }

    public void execute(Path file, long pollInterval, FileCompletionWorkerMode mode, long retriesOrTimeout) {
        this.file = file;
        this.pollInterval = pollInterval;
        this.mode = mode;
        this.retriesOrTimeout = retriesOrTimeout;
        start();
    }

    public boolean isRunning() {
        return !worker.isShutdown();
    }

    public boolean isTerminated() {
        return worker.isTerminated();
    }

    public synchronized boolean isDone() {
        return (determineFileCompletionRunnable == null ? false : determineFileCompletionRunnable.isDone());
    }

    public synchronized boolean isInterrupted() {
        return (determineFileCompletionRunnable == null ? false : determineFileCompletionRunnable.isInterrupted());
    }

    public void shutdown() {
        worker.shutdown();
        if (file != null && !isDone()) log.trace("Received data completed unsuccessfully: {} => isInterrupted({})", file.toString(), isInterrupted());
        try {
            if (!worker.awaitTermination(PathWatcher.WORKER_SHUTDOWN_TIMEOUT, TimeUnit.MILLISECONDS)) {
                worker.shutdownNow();
            }
            log.info("shutdown success");
        } catch (InterruptedException e) {
            log.error("shutdown failed",e);
        }
    }


}
