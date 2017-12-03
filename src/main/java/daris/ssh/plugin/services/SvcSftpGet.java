package daris.ssh.plugin.services;

import java.util.Collection;

import arc.xml.XmlDoc.Element;
import arc.xml.XmlWriter;
import io.github.xtman.ssh.client.Connection;
import io.github.xtman.ssh.client.SftpClient;
import io.github.xtman.ssh.client.TransferClient.GetHandler;
import io.github.xtman.util.PathUtils;

public class SvcSftpGet extends AbstractSshGetService {

    public static final String SERVICE_NAME = "daris.sftp.get";

    public SvcSftpGet() {

    }

    @Override
    protected void execute(Connection cxn, Collection<String> paths, String namespace, GetHandler gh, Element args,
            Inputs inputs, Outputs outputs, XmlWriter w) throws Throwable {
        SftpClient sftp = cxn.createSftpClient();
        try {
            for (String path : paths) {
                String parent = PathUtils.getParent(path);
                String name = PathUtils.getLastComponent(path);
                if (parent != null) {
                    sftp.setRemoteBaseDirectory(parent);
                } else {
                    sftp.setRemoteBaseDirectory(Connection.DEFAULT_REMOTE_BASE_DIRECTORY);
                }
                sftp.get(name, gh);
            }
        } finally {
            sftp.close();
        }
    }

    @Override
    public String description() {
        return "Get files from remote server to sepcified asset namespace via SFTP.";
    }

    @Override
    public String name() {
        return SvcSftpGet.SERVICE_NAME;
    }

}
