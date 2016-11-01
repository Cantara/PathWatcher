package no.cantara.file.watcher.event;

import no.cantara.file.watcher.support.FileWatchKey;
import no.cantara.file.watcher.support.FileWatchState;

import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

/**
 * Created by ora on 10/20/16.
 */
public class FileWatchEvent {

    private final Path file;
    private final FileWatchKey fileWatchKey;
    private final FileWatchState fileWatchState;
    private final BasicFileAttributes attrs;

    public FileWatchEvent(Path file, FileWatchKey fileWatchKey, FileWatchState fileWatchState, BasicFileAttributes attrs) {
        this.file = file;
        this.fileWatchKey = fileWatchKey;
        this.fileWatchState = fileWatchState;
        this.attrs = attrs;
    }

    public Path getFile() {
        return file;
    }

    public FileWatchKey getFileWatchKey() {
        return fileWatchKey;
    }

    public FileWatchState getFileWatchState() {
        return fileWatchState;
    }

    public BasicFileAttributes getAttrs() {
        return attrs;
    }

    @Override
    public String toString() {
        if (attrs == null) return "file: " + file.toString();
        StringBuffer buf = new StringBuffer();
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:ms");

        buf.append("file: " + file.toString() );
        buf.append(", fileWatchKey: " + fileWatchKey.name() );
        buf.append(", fileWatchState: " + fileWatchState.name() );
        buf.append(", creationTime: " + df.format(attrs.creationTime().toMillis()) );

        return buf.toString();
    }
}
