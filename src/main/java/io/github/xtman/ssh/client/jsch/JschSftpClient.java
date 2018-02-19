package io.github.xtman.ssh.client.jsch;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.ChannelSftp.LsEntry;
import com.jcraft.jsch.ChannelSftp.LsEntrySelector;
import com.jcraft.jsch.SftpATTRS;
import com.jcraft.jsch.SftpException;

import io.github.xtman.ssh.client.FileAttrs;
import io.github.xtman.ssh.client.SftpClient;
import io.github.xtman.util.PathUtils;

public class JschSftpClient extends JschTransferClient<ChannelSftp> implements SftpClient {

    JschSftpClient(JschConnection connection, com.jcraft.jsch.ChannelSftp channel, String remoteBaseDir,
            String encoding, Integer dirMode, Integer fileMode, boolean compress, boolean preserve, boolean verbose)
            throws Throwable {
        super(connection, channel, remoteBaseDir, encoding, dirMode, fileMode, compress, preserve, verbose);
        this.channel.connect();
        this.channel.setFilenameEncoding(encoding);
        this.channel.cd(remoteBaseDirectory());
    }

    @Override
    public void setRemoteBaseDirectory(String remoteBaseDir) throws Throwable {
        super.setRemoteBaseDirectory(remoteBaseDir);
        this.channel.cd(remoteBaseDirectory());
    }

    @Override
    public void setEncoding(String encoding) {
        try {
            this.channel.setFilenameEncoding(encoding);
            super.setEncoding(encoding);
        } catch (SftpException e) {
            e.printStackTrace();
        }
    }

    static boolean exists(com.jcraft.jsch.ChannelSftp channel, String remotePath) throws Throwable {
        try {
            channel.stat(remotePath);
            return true;
        } catch (SftpException e) {
            if (e.id == com.jcraft.jsch.ChannelSftp.SSH_FX_NO_SUCH_FILE) {
                return false;
            } else {
                throw e;
            }
        }
    }

    static void mkdirs(com.jcraft.jsch.ChannelSftp channel, String dstDirPath, Integer mode, Integer mtime)
            throws Throwable {
        if (exists(channel, dstDirPath)) {
            return;
        }
        String parentDirPath = PathUtils.getParent(dstDirPath);
        if (parentDirPath != null && !parentDirPath.equals("/") && !parentDirPath.equals(".")
                && !exists(channel, parentDirPath)) {
            mkdirs(channel, parentDirPath, mode, mtime);
        }
        channel.mkdir(dstDirPath);
        if (mode != null) {
            channel.chmod(mode, dstDirPath);
        }
        if (mtime != null) {
            channel.setMtime(dstDirPath, mtime);
        }
    }

    @Override
    public void put(FileAttrs file, InputStream in) throws Throwable {
        String path = file.path();
        if (file.isDirectory()) {
            mkdirs(this.channel, path, preserve() ? file.mode() : null, preserve() ? file.mtime() : null);
        } else {
            String parent = PathUtils.getParent(path);
            if (parent != null && !exists(this.channel, parent)) {
                mkdirs(this.channel, parent, null, null);
            }
            this.channel.put(in, path, null, com.jcraft.jsch.ChannelSftp.OVERWRITE);
            if (preserve()) {
                // TODO test
                SftpATTRS attrs = this.channel.stat(path);
                attrs.setPERMISSIONS(file.mode());
                attrs.setACMODTIME(file.atime(), file.mtime());
                this.channel.setStat(path, attrs);
            }
        }
    }

    @Override
    public void get(String remotePath, GetHandler h) throws Throwable {
        String base = PathUtils.getParent(remotePath);
        String name = PathUtils.getLastComponent(remotePath);
        get(base, name, h);
    }

    private void get(String base, String path, GetHandler h) throws Throwable {
        String remotePath = PathUtils.join(base, path);
        SftpATTRS attrs = this.channel.stat(remotePath);
        if (attrs.isDir()) {
            h.getDirectory(new FileAttrs(path, attrs.getPermissions(), attrs.getMTime(), attrs.getATime()));
            final List<LsEntry> entries = new ArrayList<LsEntry>();
            this.channel.ls(remotePath, new LsEntrySelector() {

                @Override
                public int select(LsEntry entry) {
                    String name = entry.getFilename();
                    if (!".".equals(name) && !"..".equals(name)) {
                        entries.add(entry);
                    }
                    return LsEntrySelector.CONTINUE;
                }
            });
            if (entries != null) {
                for (LsEntry entry : entries) {
                    String ePath = PathUtils.join(path, entry.getFilename());
                    if (entry.getAttrs().isDir()) {
                        SftpATTRS eAttrs = entry.getAttrs();
                        h.getDirectory(
                                new FileAttrs(ePath, eAttrs.getPermissions(), eAttrs.getMTime(), eAttrs.getATime()));
                    }
                    get(base, ePath, h);
                }
            }
        } else {
            InputStream in = this.channel.get(remotePath);
            try {
                h.getFile(new FileAttrs(path, attrs.getPermissions(), attrs.getSize(), attrs.getMTime(),
                        attrs.getATime()), in);
            } finally {
                in.close();
            }
        }
    }

    @Override
    public final String channelType() {
        return SftpClient.CHANNEL_TYPE_NAME;
    }

}
