package io.github.xtman.ssh.client;

public interface Executor extends Channel {

    public static final String CHANNEL_TYPE_NAME = "exec";

    void execute(String command) throws Throwable;

}
