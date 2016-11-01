package no.cantara.file.watcher;

import no.cantara.file.watcher.support.FileCompletionWorkerMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Created by ora on 10/20/16.
 */
public class FileWatchUtils {

    private final static Logger log = LoggerFactory.getLogger(FileWatchUtils.class);
    private static Path currentPath;

    public static Path getCurrentPath() {
        return Paths.get("").toAbsolutePath();
    }

    public static void createDirectories(Path directory) throws IOException {
        if (!Files.isDirectory(directory)) {
            Path dir;
            if ((dir = Files.createDirectories(directory)) != null) {
                log.trace("Created directory: {}", dir.toString());
            } else {
                log.trace("Unable to create directory: {}", dir.toString());
            }
        }
    }

    public static OutputStream readFile(Path file) throws IOException {
        ByteArrayOutputStream baos = null;
        FileInputStream fis = new FileInputStream(file.toFile());
        try {
            baos = new ByteArrayOutputStream();
            int read;
            byte[] bytes = new byte[8192];
            while ((read = fis.read(bytes)) != -1) {
                baos.write(bytes, 0, read);
            }
        } finally {
            fis.close();
        }
        return baos;
    }

    public static OutputStream readFile(Path dir, Path filename) throws IOException {
        Path file = dir.resolve(filename);
        return readFile(file);
    }

    public static void writeTo(OutputStream source, Path file) throws IOException {
        FileOutputStream out = new FileOutputStream(file.toFile());
        try {
            FileLock lock = out.getChannel().lock();
            try {
                ByteArrayInputStream bais = new ByteArrayInputStream(source.toString().getBytes());
                int read;
                byte[] bytes = new byte[8192];
                while ((read = bais.read(bytes)) != -1) {
                    out.write(bytes, 0, read);
                }
                bais.close();
                source.close();
            } finally {
                lock.release();
            }
        } finally {
            out.close();
            log.trace("Wrote new file: {}", file.toString());
        }
    }

    public static void writeTo(OutputStream source, Path dir, String filename) throws IOException {
        Path file = dir.resolve(filename);
        writeTo(source, file);
    }

    public static void removeFile(Path file) throws IOException {
        RandomAccessFile raf = new RandomAccessFile(file.toFile(), "rw");
        try {
            FileLock lock = raf.getChannel().lock();
            try {
                Files.delete(file);

            } finally {
                lock.release();
            }
        } finally {
            raf.close();
            log.trace("Removed file: {}", file.toString());
        }
    }

    public static void removeFile(Path dir, Path filename) throws IOException {
        Path file = dir.resolve(filename);
        removeFile(file);
    }

    public static void copy(Path sourceFile, Path targetFile) throws IOException {
        OutputStream readData = null;
        try {
            readData = readFile(sourceFile);
            writeTo(readData, targetFile);
        } catch (IOException e) {
            throw new IOException(e);
        }
    }

    public static void move(Path sourceFile, Path targetFile) throws IOException {
        try {
            copy(sourceFile, targetFile);
            removeFile(sourceFile);
        } catch (IOException e) {
            throw new IOException(e);
        }
    }

    public static boolean waitForCompletion(Path file, long pollInterval, FileCompletionWorkerMode mode, long retryOrTimeout) throws InterruptedException {
        FileDetermineCompletionWorker fileDetermineCompletionWorker = new FileDetermineCompletionWorker();
        try {
            fileDetermineCompletionWorker.execute(file, pollInterval, mode, retryOrTimeout);
            while (!fileDetermineCompletionWorker.isDone() && !fileDetermineCompletionWorker.isInterrupted()) {
                Thread.sleep(pollInterval);
            }

        } finally {
            fileDetermineCompletionWorker.shutdown();
        }
        return fileDetermineCompletionWorker.isDone();
    }

    public static boolean isRelativePathFormat(String dir) {
        if (dir != null) {
            if (dir.startsWith("/")) return false;
        }
        return true;
    }
}
