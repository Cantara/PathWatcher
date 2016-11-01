# Path Watcher

Short doc PathWatcher is:

* a singleton that currently is limited to listenting to one `watchDir` only
* it uses worker threads to and event based i/o to produce new file discoveries
* disocvered files are handled in a seperate consumer thread that invokes call back events
* callbacks are not thread-safe!
* supports both Native and Poll based FileSystem. Polling is used to support MacOS/OSX (because NativeFileSystem support is not available in JDKs!)
* By default:
  * PatchWatcher will determine if Native I/O is possible. If not, it will autoamtically go for PollBased
  * Native and Poll based mode comes with two differnet configurations
* In test it writes to the directory `target/watcher`
* To understand the polling feature, study the test cases for polling configuration and (!!) => `FilePollEventsProducer` and algo in`FileDetermineCompletionWorker`

Known limitaitons:

* Has not been tested on Windows yet
* Does not support multiple watch dirs

## Use

### Default

```java
PathWatcher pw = PathWatcher.getInstance();

pw.watch(watchDir);

pw.registerCreatedHandler(new CreatedHandler());
pw.registerModifiedHandler(new ModifiedHandler());
pw.registerRemovedHandler(new RemovedHandler());

pw.start();
Thread.sleep(5000);
pw.stop();
```

### Native FileSystem

```java
PathWatcher pw = PathWatcher.getInstance();
pw.forceFileSystemScannerMode(PathWatchScanner.NATIVE_FILE_SYSTEM); // not necessary on Linux and Windows

pw.watch(watchDir);

pw.registerCreatedHandler(new CreatedHandler());
pw.registerModifiedHandler(new ModifiedHandler());
pw.registerRemovedHandler(new RemovedHandler());

pw.start();
Thread.sleep(5000);
pw.stop();
```

### Poll FileSystem

```java
PathWatcher pw = PathWatcher.getInstance();
pw.forceFileSystemScannerMode(PathWatchScanner.POLL_FILE_SYSTEM); // will always be the case on MacOS
pw.setPathScanInterval(1000);
pw.setThreadPollInterval(150);
pw.setWorkerMode(FileCompletionWorkerMode.TIMEOUT);
pw.setTimeoutOrRetryInterval(500);

pw.watch(watchDir);

pw.registerCreatedHandler(new CreatedHandler());
pw.registerModifiedHandler(new ModifiedHandler());
pw.registerRemovedHandler(new RemovedHandler());

pw.start();
Thread.sleep(5000);
pw.stop();
```


#### CreateHandler

```java
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
```

#### ModifyHandler

```java
public static class ModifiedHandler implements FileWatchHandler {
    @Override
    public void invoke(FileWatchEvent event) {
        Path file = event.getFile();
        log.trace("OnModifiedFileAction - Received FileWatchEvent from Consumer: {}", file);
    }
}
```

#### RemoveHandler

```java
public static class RemovedHandler implements FileWatchHandler {
    @Override
    public void invoke(FileWatchEvent event) {
        Path file = event.getFile();
        log.trace("OnRemovedFileHandler - Received FileWatchEvent from Consumer: {}", file);
    }
}
```
