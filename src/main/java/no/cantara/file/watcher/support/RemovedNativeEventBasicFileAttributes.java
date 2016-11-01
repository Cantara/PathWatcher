package no.cantara.file.watcher.support;

import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.concurrent.TimeUnit;

/**
 * Created by ora on 10/21/16.
 */
public class RemovedNativeEventBasicFileAttributes implements BasicFileAttributes {

    private static final Calendar now = now();

    private static Calendar now() {
        Calendar cal = GregorianCalendar.getInstance();
        return cal;
    }

    @Override
    public FileTime lastModifiedTime() {
        return FileTime.from(now.getTimeInMillis(), TimeUnit.MILLISECONDS);
    }

    @Override
    public FileTime lastAccessTime() {
        return FileTime.from(now.getTimeInMillis(), TimeUnit.MILLISECONDS);
    }

    @Override
    public FileTime creationTime() {
        return FileTime.from(now.getTimeInMillis(), TimeUnit.MILLISECONDS);
    }

    @Override
    public boolean isRegularFile() {
        return true;
    }

    @Override
    public boolean isDirectory() {
        return false;
    }

    @Override
    public boolean isSymbolicLink() {
        return false;
    }

    @Override
    public boolean isOther() {
        return false;
    }

    @Override
    public long size() {
        return -1;
    }

    /**
     * It's okay to return void, because it a removed file is not unique anymore. Eliminates NPE, but case ClassCastException
     *
     * @return
     */
    @Override
    public Object fileKey() {
        return Void.TYPE;
    }
}
