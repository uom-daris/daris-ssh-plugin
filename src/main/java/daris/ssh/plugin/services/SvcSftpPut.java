package daris.ssh.plugin.services;

import io.github.xtman.ssh.client.Connection;
import io.github.xtman.ssh.client.SftpClient;

public class SvcSftpPut extends AbstractSshPutService {

    public static final String SERVICE_NAME = "daris.sftp.put";

    @Override
    protected SftpClient createTransferClient(Connection cxn, String directory) throws Throwable {
        return cxn.createSftpClient(directory);
    }

    @Override
    public String description() {
        return "Send specified assets to remote server via sftp";
    }

    @Override
    public String name() {
        return SERVICE_NAME;
    }

}
