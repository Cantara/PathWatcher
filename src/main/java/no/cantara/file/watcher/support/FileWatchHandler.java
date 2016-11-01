package no.cantara.file.watcher.support;

import no.cantara.file.watcher.event.FileWatchEvent;

/**
 * Created by oranheim on 20/10/2016.
 */
public interface FileWatchHandler {

    void invoke(FileWatchEvent event);

}
