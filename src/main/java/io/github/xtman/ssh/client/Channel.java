package io.github.xtman.ssh.client;

import java.io.Closeable;

public interface Channel extends Closeable {

    Connection connection();

    String channelType();

    String remoteBaseDirectory();

    void setRemoteBaseDirectory(String baseDir) throws Throwable;

    String encoding();

    void setEncoding(String encoding);
    
    boolean verbose();

    void setVerbose(boolean verbose);

}
