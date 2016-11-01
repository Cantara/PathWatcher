package no.cantara.file.watcher;

import java.nio.file.FileSystems;

/**
 * Created by ora on 10/20/16.
 */
public class FileSystemSupport {

    public static String getOSString() {
        return System.getProperties().get("os.name") + " " + System.getProperties().get("os.version");
    }

    public static boolean isLinux() {
        return (getOSString().indexOf("Linux") > -1);
    }

    public static boolean isLinuxFileSystem() {
        return (isLinux() && "LinuxFileSystem".equals(FileSystems.getDefault().getClass().getSimpleName()));
    }

    public static boolean isMacOS() {
        return ( (getOSString().indexOf("MacOS") > -1) || (getOSString().indexOf("OS X") > -1) );
    }

    public static boolean isMacOSFileSystem() {
        return (isMacOS() && "MacOSXFileSystem".equals(FileSystems.getDefault().getClass().getSimpleName()));
    }

    public static boolean hasJDKEventDrivenFileSystem() {
        return (isLinuxFileSystem() && !isMacOSFileSystem());
    }

}
