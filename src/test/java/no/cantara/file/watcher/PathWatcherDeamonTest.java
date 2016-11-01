package no.cantara.file.watcher;

import no.cantara.file.util.CommonUtil;
import no.cantara.file.watcher.event.FileWatchEvent;
import no.cantara.file.watcher.support.FileWatchHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.OutputStream;
import java.nio.file.Path;

/**
 * Created by ora on 11/1/16.
 *
 * Enable this test case to start listening on ${basedir}/watcher/store/
 * Copy a file into the inbox and check outcome in archive and unsupportedArchive
 *
 * E.g.
 *
 * <pre>
 * $ cp src/test/resources/test-file.xml watcher/store/inbox/
 * $ cp src/test/resources/test-file-fail.xml watcher/store/inbox/
 * $ tree watcher/store/
 * </pre>
 *
 */
@Test(enabled = false)
public class PathWatcherDeamonTest {

    private final static Logger log = LoggerFactory.getLogger(PathWatcherDeamonTest.class);
    private static final long WAIT_UNTIL_INTERRUPT = (System.getenv().containsKey("JENKINS_VERSION") ? 5000 : 2500);

    private PathWatcher storeWatcher;
    private static Path STORE_PATH = FileWatchUtils.getCurrentPath().resolve("watcher/store");
    private static Path INBOX_PATH = STORE_PATH.resolve("inbox");
    private static Path ARCHIVE_PATH = STORE_PATH.resolve("archive");
    private static Path UNSUPPORTED_ARCHIVE_PATH = STORE_PATH.resolve("unsupportedArchive");

    private static class FileCreatedStoreHandler implements FileWatchHandler {
        @Override
        public void invoke(FileWatchEvent fileWatchEvent) {
            try {
                OutputStream data = FileWatchUtils.readFile(fileWatchEvent.getFile());
                String payload = data.toString();
                if (payload.contains("catalog")) {

                    PathWatcher store = PathWatcher.getInstance();
                    Path sourceFile = fileWatchEvent.getFile();


                    Path archiveFile = ARCHIVE_PATH.resolve(sourceFile.getFileName());

                    log.trace("Archiving file from '{}' to '{}'", sourceFile, archiveFile);
                    FileWatchUtils.move(sourceFile, archiveFile);
                } else {
                    try {
                        throw new UnsupportedOperationException("Error with data");
                    } catch (UnsupportedOperationException e) {
                        try {
                            OutputStream err = CommonUtil.newOutputStream();
                            err.write(e.fillInStackTrace().toString().getBytes());
                            PathWatcher store = PathWatcher.getInstance();
                            Path sourceFile = fileWatchEvent.getFile();
                            Path unsupportedArchiveFile = UNSUPPORTED_ARCHIVE_PATH.resolve(sourceFile.getFileName());
                            log.warn("Archiving Unsupported file from '{}' to '{}'", sourceFile, unsupportedArchiveFile);
                            FileWatchUtils.move(sourceFile, unsupportedArchiveFile);
                            Path unsupportedArchiveErrFile = UNSUPPORTED_ARCHIVE_PATH.resolve(sourceFile.getFileName() + ".err");
                            FileWatchUtils.writeTo(err, unsupportedArchiveErrFile);
                        } catch (Exception ex) {
                            log.error("{}", e);
                        }
                        log.error("{}", e);
                    }
                }
            } catch (Exception e) { // IOException
                log.error("{}", e);
            }
        }
    }


    @BeforeClass
    public void before() throws Exception {
        storeWatcher = PathWatcher.getInstance();
        FileWatchUtils.createDirectories(INBOX_PATH);
        FileWatchUtils.createDirectories(ARCHIVE_PATH);
        FileWatchUtils.createDirectories(UNSUPPORTED_ARCHIVE_PATH);
        storeWatcher.watch(INBOX_PATH);
        storeWatcher.setWorkerShutdownTimeout(WAIT_UNTIL_INTERRUPT);
        storeWatcher.registerCreatedHandler(new FileCreatedStoreHandler());
        storeWatcher.start();
    }

    @AfterClass
    public void after() throws Exception {
        storeWatcher.stop();
        Thread.sleep(250);
        storeWatcher = null;
    }

    @Test
    public void testMe() throws InterruptedException {
        if (storeWatcher == null) return;
        Thread.sleep((60 * 1000));
    }

}
