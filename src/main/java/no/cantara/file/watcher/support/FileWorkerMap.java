package no.cantara.file.watcher.support;

import com.google.common.collect.*;
import no.cantara.file.watcher.event.FileWatchEvent;

import java.nio.file.Path;
import java.util.Collection;
import java.util.Iterator;

/**
 * Created by ora on 10/20/16.
 *
 * todo: must be changed. Read: http://dhruba.name/2011/06/11/concurrency-pattern-concurrent-multimaps-in-java/
 * http://stackoverflow.com/questions/1572178/guava-multimap-and-concurrentmodificationexception
 */
public class FileWorkerMap {

    private final Multimap<Path,FileWatchEvent> map = ArrayListMultimap.create();

    public FileWorkerMap() {
    }

    // unused
    private Multimap<Path,FileWatchEvent> map() {
        synchronized (map) {
            return map;
        }
    }

    public synchronized Multiset<Path> keys() {
        return Multisets.unmodifiableMultiset(map.keys());
    }

    public synchronized ImmutableList<Path> keySet() {
        return ImmutableList.copyOf(map.keySet());
    }

    //public boolean isFileCreated()


    public synchronized void add(Path file, FileWatchEvent fileWatchEvent) {
        map.put(file, fileWatchEvent);
    }

    public synchronized void remove(Path file) {
        map.removeAll(file);
    }

    public synchronized int size() {
        return map.keySet().size();
    }

    public synchronized boolean isEmpty() {
        return map.isEmpty();
    }

    public synchronized void clear() {
        map.clear();
    }

    public synchronized boolean checkState(Path file, FileWatchState state) {
        Collection<FileWatchEvent> entries = map.get(file);
        Iterator it = entries.iterator();
        while(it.hasNext()) {
            FileWatchEvent value = (FileWatchEvent) it.next();
            if(state.equals(value.getFileWatchState())) {
                return true;
            }
        }
        return false;
    }

    public synchronized boolean checkState(Path file, FileWatchKey fileWatchKey) {
        Collection<FileWatchEvent> entries = map.get(file);
        Iterator it = entries.iterator();
        while(it.hasNext()) {
            FileWatchEvent value = (FileWatchEvent) it.next();
            if(fileWatchKey.equals(value.getFileWatchKey())) {
                return true;
            }
        }
        return false;
    }

    // todo: check that it returns the last file in the value chain
    public synchronized FileWatchEvent getFile(Path file) {
        Collection<FileWatchEvent> entries = map.get(file);
        Iterator it = entries.iterator();
        while(it.hasNext()) {
            FileWatchEvent value = (FileWatchEvent) it.next();
            if(file.equals(value.getFile())) {
                return value;
            }
        }
        return null;
    }

    public synchronized Collection<FileWatchEvent> getFileEntries(Path file) {
        return map.get(file);
    }

}
