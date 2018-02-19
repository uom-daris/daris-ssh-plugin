package io.github.xtman.ssh.client;

public interface ScpClient extends TransferClient, Channel {

    public static final String CHANNEL_TYPE_NAME = Executor.CHANNEL_TYPE_NAME;

}
