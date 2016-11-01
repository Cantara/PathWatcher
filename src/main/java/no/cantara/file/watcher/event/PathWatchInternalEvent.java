package no.cantara.file.watcher.event;

import java.lang.ref.WeakReference;

/**
 * Created by oranheim on 19/10/2016.
 */
public class PathWatchInternalEvent {

    private final WeakReference<Runnable> source;
    private final String message;

    public PathWatchInternalEvent(Runnable source, String message) {
        this.source = new WeakReference<>(source);
        this.message = message;
    }

    public WeakReference<Runnable> getSource() {
        return source;
    }

    public String getMessage() {
        return message;
    }
}
