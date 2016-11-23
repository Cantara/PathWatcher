package no.cantara.file.watcher;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import no.cantara.file.watcher.event.FileWatchEvent;
import no.cantara.file.watcher.support.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileLock;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.testng.Assert.*;

/**
 * Created by oranheim on 19/10/2016.
 */
public class PathWatchTest {

    private final static Logger log = LoggerFactory.getLogger(PathWatchTest.class);

    @Test(enabled = true)
    public void testFileSystem() {
        log.trace("IsLinux: {}, isLinuxFileSystem: {}, isMacOS: {}, isMacOSFileSystem: {}",
                FileSystemSupport.isLinux(),
                FileSystemSupport.isLinuxFileSystem(),
                FileSystemSupport.isMacOS(),
                FileSystemSupport.isMacOSFileSystem());;
        log.trace("hasJDKEventDrivenFileSystem: {}", FileSystemSupport.hasJDKEventDrivenFileSystem());
    }

    public static class CreatedHandler implements FileWatchHandler {
        @Override
        public void invoke(FileWatchEvent event) {
            Path file = event.getFile();
            log.trace("OnCreatedFileAction - Received FileWatchEvent from Consumer: {}", file);
            try {
                if (Files.exists(file))
                    Files.delete(file);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static class ModifiedHandler implements FileWatchHandler {
        @Override
        public void invoke(FileWatchEvent event) {
            Path file = event.getFile();
            log.trace("OnModifiedFileAction - Received FileWatchEvent from Consumer: {}", file);
        }
    }

    public static class RemovedHandler implements FileWatchHandler {
        @Override
        public void invoke(FileWatchEvent event) {
            Path file = event.getFile();
            log.trace("OnRemovedFileHandler - Received FileWatchEvent from Consumer: {}", file);
        }
    }

    @Test(enabled = true, dependsOnGroups = "pre-test")
    public void testNativeEventsPathWatch() throws Exception {
        if (FileSystemSupport.isMacOS()) return;

        Path currentDir = FileWatchUtils.getCurrentPath();
        Path storeDir = currentDir.resolve("target/watcher");
        Path inboxDir = storeDir.resolve("inbox");
        FileWatchUtils.createDirectories(inboxDir);

        PathWatcher pw = PathWatcher.getInstance();
        //pw.forceFileSystemScannerMode(PathWatchScanner.NATIVE_FILE_SYSTEM); // it always will fallback on appropriate scanner mode

        execPathWatchTest(pw, inboxDir);
    }

    @Test(enabled = true, dependsOnGroups = "pre-test")
    public void testPollEventsPathWatch() throws Exception {
        Path currentDir = FileWatchUtils.getCurrentPath();
        Path storeDir = currentDir.resolve("target/watcher");
        Path inboxDir = storeDir.resolve("inbox");
        FileWatchUtils.createDirectories(inboxDir);

        PathWatcher pw = PathWatcher.getInstance();
        pw.forceFileSystemScannerMode(PathWatchScanner.POLL_FILE_SYSTEM);
        pw.setPathScanInterval(1000);
        pw.setThreadPollInterval(150);
        pw.setWorkerMode(FileCompletionWorkerMode.TIMEOUT);
        pw.setTimeoutOrRetryInterval(500);

        execPathWatchTest(pw, inboxDir);
    }

    private void execPathWatchTest(PathWatcher pw, Path inboxDir) throws IOException, InterruptedException {
        pw.watch(inboxDir);

        pw.registerCreatedHandler(new CreatedHandler());
        pw.registerModifiedHandler(new ModifiedHandler());
        pw.registerRemovedHandler(new RemovedHandler());

        pw.start();
        assertTrue(pw.isRunning());

        String fn = inboxDir.toFile() + "/a1.txt";
        log.trace("FN: {}", fn);
        FileOutputStream fis = new FileOutputStream(fn);
        fis.write("Test 1".getBytes());
        fis.close();

        fn = inboxDir.toFile() + "/a2.txt";
        log.trace("FN: {}", fn);
        fis = new FileOutputStream(fn);
        fis.write("Test 2".getBytes());
        fis.close();

        Thread.sleep(2500);

        log.trace("STOPPING..");
        pw.stop(); // blocking call
        log.trace("STOPPED!");

    }

    @Test(enabled = true, groups = "pre-test")
    public void testCreateUniqueMapToHandleMultipleDiscoveriesOfFiles() throws Exception {
        Multimap<String,String> map =  ArrayListMultimap.create();

        map.put("a", "a");
        map.put("a", "b");
        map.put("a", "c");

        map.put("b", "b");
        map.put("b", "c");

        // all keys and values
        map.keys().forEach(k -> {
            List<String> v = Lists.newArrayList(map.get(k));
            log.trace("KEYS => key={}, value={}, size={}", k, v.toString(), v.size());
        });

        assertTrue( map.containsKey("a") );
        assertTrue( map.containsKey("b") );
        assertFalse( map.containsKey("c") );

        assertEquals( 3, map.get("a").size() );
        assertEquals( 2, map.get("b").size() );

        // unique set and values
        map.keySet().forEach(k -> {
            List<String> v = Lists.newArrayList(map.get(k));
            log.trace("KEYSET: key={}, value={}, size={}", k, v.toString(), v.size());
        });

        assertEquals( 5, map.keys().size() );
        assertEquals( 2, map.keySet().size() );

        /**
         * Key = filenamepath (the file is our handle)
         * Value = FileWatchEvent (descriptor info)
         *
         * Rules:
         * - First found create (CREATED, UNKNOWN)
         * - Second check the FileWorkerMap and check if the file has been already discovered. If so, add MODIFIED, UNNKNOWN)
         * - Third, add states for INCOMPLETE(meaning not fully readable, locked, when we read/write, and completed when done)
         *
         * Need a FileWorkerMap:
         * .getState
         * .exists // file already is discovered
         * .add(FileWatchEvent)
         * .remove(FileWatchEvent) // when done
         */

        Multimap<String,String> map2 =  ArrayListMultimap.create();
        map2.putAll(map);
        assertEquals(map.size(), map2.size());

        map2.removeAll("a");
        assertEquals(1, map2.keySet().size());

        Iterator<String> it = map.keySet().iterator();
        while(it.hasNext()) {
            String key = it.next();
            if (map2.keySet().contains(key)) {
                continue;
            } else {
                // we have a removed item
                log.trace("Item {} was removed!", key);
            }
        }
    }

    public class WriteLongFileRunnable implements Runnable {

        private final Path file;
        private boolean done = false;
        private boolean sleepOnce;

        public WriteLongFileRunnable(Path file, boolean sleepOnce) {
            this.file = file;
            this.sleepOnce = !sleepOnce;
        }

        @Override
        public void run() {

            try {
                FileOutputStream fos = null;
                FileLock fileLock = null;
                try {
                    fos = new FileOutputStream(file.toFile());
                    fileLock = fos.getChannel().lock();
                    int numbOfRandomBytes = 1024 * 1024 * 10; // 10 Mb
                    Random r = new Random(26);
                    log.trace("----------------------> Start writing file {}...", file.toString());
                    for(int n = 0; n<(numbOfRandomBytes/64); n++) {
                        byte[] bytes = new byte[64];
                        for(int m = 0; m<64; m++) {
                            char c = (char) (r.nextInt(65) + 'a');
                            bytes[m] = (byte) c;
                        }
                        fos.write(bytes, 0, 64);

                        if ( !sleepOnce && (n > ( (numbOfRandomBytes / 64) / 2) ) ) {
                            sleepOnce = true;
                            try {
                                log.trace("----------------------> Let's sleep for a while while writing");
                                Thread.sleep(2000);
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        }
                    }
                } finally {
                    if (fos != null) {
                        fos.flush();
                        if (fileLock != null) {
                            fileLock.release();
                        }
                        fos.close();
                    }
                    done = true;
                    log.trace("----------------------> Long File is written to: {}", file.toString());
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        public boolean isDone() {
            return done;
        }
    }

    @Test(enabled=true)
    public void testFileTakesLongTimeToWrite() throws Exception {
        String fileName = "veryLargeFile.txt";
        Path currentDir = FileWatchUtils.getCurrentPath();
        Path watchDir = currentDir.resolve("target/watcher/inbox");
        Path slowFile = watchDir.resolve(fileName);
        FileWatchUtils.createDirectories(watchDir) ;

        PathWatcher pathWatcher = PathWatcher.getInstance();
        pathWatcher.registerCreatedHandler(event -> {
            log.info("Received event; {}", event);
            Path eventFile = event.getFile();
            Assert.assertTrue(fileName.equals(event.getFile().getFileName().toString()));
            Assert.assertTrue(Files.exists(eventFile));
            Assert.assertTrue(Files.isReadable(eventFile));
        });
        pathWatcher.watch(watchDir);
        pathWatcher.start();

        do {
            waitSomeTime(300);
        }  while (!pathWatcher.isRunning());
        log.info("pathWatcher isRunning {}", pathWatcher.isRunning());

        WriteLongFileRunnable longFileRunnable = new WriteLongFileRunnable(slowFile, false);
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        executorService.execute(longFileRunnable);

        do {
            waitSomeTime(300);
        } while (! longFileRunnable.isDone());

        waitSomeTime(PathWatcher.DELAY_QUEUE_DELAY_TIME + 300);  // wait for message to get through the delay queue
        //cleanup
        pathWatcher.stop();
        executorService.shutdown();

        if (Files.exists(slowFile)) {
            try {
                Files.delete(slowFile);
                log.debug("File deleted");
            } catch (IOException e) {
                //
            }
        }
    }

    private void waitSomeTime(long waitingTime) {
        try {
            Thread.sleep(waitingTime);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Test(enabled=true)
    public void testDirectoryCreationIsIgnored() throws Exception {
        Path currentDir = FileWatchUtils.getCurrentPath();
        Path watchDir = currentDir.resolve("target/watcher/inbox");
        Path newDir = watchDir.resolve("new_directory");
        FileWatchUtils.createDirectories(watchDir) ;

        PathWatcher pathWatcher = PathWatcher.getInstance();
        pathWatcher.registerCreatedHandler(event -> {
            log.info("Received event; {}", event);
            assertNotEquals(newDir.getFileName().toString(), event.getFile().getFileName().toString(), String.format("Should not receive this event: %s - %s - isDirectory: %s",event.getFileWatchKey(), event.getFile().getFileName(), Files.isDirectory(event.getFile())));
        });
        pathWatcher.watch(watchDir);
        pathWatcher.start();

        do {
            waitSomeTime(300);
        }  while (!pathWatcher.isRunning());
        log.info("pathWatcher isRunning {}", pathWatcher.isRunning());

        //create a new entry event for directory
        FileWatchUtils.createDirectories(newDir);
        waitSomeTime(PathWatcher.DELAY_QUEUE_DELAY_TIME + 100);

        //cleanup
        pathWatcher.stop();

        if (Files.exists(newDir)) {
            try {
                Files.delete(newDir);
                log.debug("{} deleted", newDir);
            } catch (IOException e) {
                //
            }
        }


    }


    @Test(enabled = true, groups = "pre-test")
    public void testFileWorkerMap() throws Exception {
        FileWorkerMap fileWorkerMap = new FileWorkerMap();

        Path currentDir = FileWatchUtils.getCurrentPath();
        Path storeDir = currentDir.resolve("target/watcher");
        Path inboxDir = storeDir.resolve("dummy");
        FileWatchUtils.createDirectories(inboxDir);

        Path file1 = inboxDir.resolve("f1.txt");
        FileOutputStream fos1 = new FileOutputStream(file1.toFile());
        fos1.write("fos1".getBytes());
        BasicFileAttributes attrs1 = Files.readAttributes(file1, BasicFileAttributes.class);
        fos1.close();

        Path file2 = inboxDir.resolve("f2.txt");
        FileOutputStream fos2 = new FileOutputStream(file2.toFile());
        fos2.write("fos2".getBytes());
        BasicFileAttributes attrs2 = Files.readAttributes(file2, BasicFileAttributes.class);
        fos2.close();

        Path file3 = inboxDir.resolve("f3.txt");
        WriteLongFileRunnable longFile = new WriteLongFileRunnable(file3, true);
        long past = System.currentTimeMillis();
        new Thread(longFile).start();

        FileWatchEvent fileWatchEvent1Created = new FileWatchEvent(file1, FileWatchKey.FILE_CREATED, FileWatchState.UNKNOWN, attrs1);
        FileWatchEvent fileWatchEvent1Modified = new FileWatchEvent(file1, FileWatchKey.FILE_MODIFY, FileWatchState.UNKNOWN, attrs1);
        FileWatchEvent fileWatchEvent1Removed = new FileWatchEvent(file1, FileWatchKey.FILE_REMOVED, FileWatchState.UNKNOWN, attrs1);
        FileWatchEvent fileWatchEvent2Created = new FileWatchEvent(file1, FileWatchKey.FILE_CREATED, FileWatchState.UNKNOWN, attrs2);
        FileWatchEvent fileWatchEvent2Removed = new FileWatchEvent(file1, FileWatchKey.FILE_REMOVED, FileWatchState.UNKNOWN, attrs2);

        log.trace("fileWatchEvent1Created: {}", fileWatchEvent1Created.toString());
        log.trace("fileWatchEvent1Modified: {}", fileWatchEvent1Modified.toString());
        log.trace("fileWatchEvent1Removed: {}", fileWatchEvent1Removed.toString());
        log.trace("fileWatchEvent2Created: {}", fileWatchEvent2Created.toString());
        log.trace("fileWatchEvent2Removed: {}", fileWatchEvent2Removed.toString());

        fileWorkerMap.add(file1, fileWatchEvent1Created);
        fileWorkerMap.add(file1, fileWatchEvent1Modified);
        fileWorkerMap.add(file1, fileWatchEvent1Removed);
        assertEquals(1, fileWorkerMap.size());

        fileWorkerMap.add(file2, fileWatchEvent2Created);
        fileWorkerMap.add(file2, fileWatchEvent2Removed);
        assertEquals(2, fileWorkerMap.size());

        fileWorkerMap.remove(file1);
        assertEquals(1, fileWorkerMap.size());

        fileWorkerMap.remove(file2);
        assertTrue(fileWorkerMap.isEmpty());

        // -- starting fresh again

        fileWorkerMap = new FileWorkerMap();
        FileWatchEvent fileWatchEvent3Created;
        BasicFileAttributes attrs3;

        boolean workerStarted = false;
        boolean success = false;
        while(!longFile.isDone()) {
            Thread.sleep(100);
            if (!workerStarted) {
                workerStarted = true;
                success = FileWatchUtils.waitForCompletion(file3, 100, FileCompletionWorkerMode.PASSTHROUGH, -1); // if it won't be able to await file3 to complete, it'll fail due to file3 2500ms timeout
                assertFalse(success);
                log.trace("The 'file3' instance was not completed by the FileWatchUtils.waitForCompletion ON PURPOSE!");
                attrs3 = Files.readAttributes(file3, BasicFileAttributes.class);
                fileWatchEvent3Created = new FileWatchEvent(file3, FileWatchKey.FILE_CREATED, FileWatchState.INCOMPLETE, attrs3);
                fileWorkerMap.add(file3, fileWatchEvent3Created);
                assertTrue(fileWorkerMap.checkState(file3, FileWatchState.INCOMPLETE));
                assertFalse(fileWorkerMap.checkState(file3, FileWatchState.COMPLETED));
            }
        }

        long future = System.currentTimeMillis();

        // check that timeout mode work and add a completed file to map
        success = FileWatchUtils.waitForCompletion(file3, 100, FileCompletionWorkerMode.TIMEOUT, 500);
        assertTrue(success);
        attrs3 = Files.readAttributes(file3, BasicFileAttributes.class);
        fileWatchEvent3Created = new FileWatchEvent(file3, FileWatchKey.FILE_CREATED, FileWatchState.COMPLETED, attrs3);
        fileWorkerMap.add(file3, fileWatchEvent3Created);
        assertTrue(fileWorkerMap.checkState(file3, FileWatchState.COMPLETED));

        // check that retries mode work
        success = FileWatchUtils.waitForCompletion(file3, 100, FileCompletionWorkerMode.RETRIES, 20);
        assertTrue(success);

        if (longFile.isDone())
            log.trace("Successfully completed writing a really long file: {}ms", (future-past));
        else
            log.trace("Unsuccessfully completed writing a really long file: {}ms !!!", (future-past));


        // -- new thread write

        Path file4 = inboxDir.resolve("f4.txt");
        longFile = new WriteLongFileRunnable(file4, false);
        past = System.currentTimeMillis();
        new Thread(longFile).start();
        int count = 0;
        while(!longFile.isDone()) {
            log.trace("Loop File4 iteration: ", count);
            success = FileWatchUtils.waitForCompletion(file4, 100, FileCompletionWorkerMode.TIMEOUT, 500);
            log.trace("File4 outcome was: {}", success);;
            count++;
        }
        future = System.currentTimeMillis();

        if (longFile.isDone())
            log.trace("File4: Successfully completed writing a really long file: {}ms", (future-past));
        else
            log.trace("File4: Unsuccessfully completed writing a really long file: {}ms !!!", (future-past));


    }
}
