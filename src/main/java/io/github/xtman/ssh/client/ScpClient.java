package io.github.xtman.ssh.client;

public interface ScpClient extends TransferClient, Channel {

    public static final String CHANNEL_TYPE_NAME = Executor.CHANNEL_TYPE_NAME;

    public static final int DEFAULT_DIRECTORY_MODE = 0755;

    public static final int DEFAULT_FILE_MODE = 0644;

}
