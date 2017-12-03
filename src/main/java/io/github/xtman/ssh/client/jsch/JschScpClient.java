package io.github.xtman.ssh.client.jsch;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Stack;

import com.jcraft.jsch.ChannelExec;

import io.github.xtman.io.util.SizedInputStream;
import io.github.xtman.io.util.StreamUtils;
import io.github.xtman.ssh.client.Connection;
import io.github.xtman.ssh.client.Executor;
import io.github.xtman.ssh.client.FileAttrs;
import io.github.xtman.ssh.client.ScpClient;
import io.github.xtman.util.PathUtils;

public class JschScpClient extends JschTransferClient<ChannelExec> implements ScpClient {

    private boolean _initializedPut = false;

    private InputStream _cin;
    private OutputStream _cout;

    JschScpClient(JschConnection connection, ChannelExec channel, String remoteBaseDir, String encoding,
            Integer dirMode, Integer fileMode, boolean compress, boolean preserve, boolean verbose) throws Throwable {
        super(connection, channel, remoteBaseDir, encoding, dirMode, fileMode, compress, preserve, verbose);
        _cin = new BufferedInputStream(channel.getInputStream(), 8192);
        _cout = new BufferedOutputStream(channel.getOutputStream());
    }

    @Override
    public void close() throws IOException {
        try {
            if (_cout != null) {
                if (this.verbose) {
                    System.out.print("closing output stream... ");
                }
                _cout.close();
                if (this.verbose) {
                    System.out.println("done");
                }
            }
        } finally {
            super.close();
        }
    }

    @Override
    public void put(FileAttrs file, InputStream in) throws Throwable {
        initializePut();

        String path = file.path();
        if (remoteBaseDirectory() != null && !remoteBaseDirectory().isEmpty()) {
            path = PathUtils.join(remoteBaseDirectory(), path);
        }
        String parent = PathUtils.getRelativePath(PathUtils.getParent(path), remoteBaseDirectory());
        String[] parentComponents = parent == null ? null : PathUtils.trimSlash(parent).split("/+");
        if (parentComponents != null) {
            for (int i = 0; i < parentComponents.length; i++) {
                sendPushDir(parentComponents[i]);
            }
        }
        if (file.isDirectory()) {
            sendPushDir(file.name());
            sendPopDir();
        } else {
            /*
             * send file
             */
            if (preserve() && file.mtime() != null && file.atime() != null) {
                // send time
                String cmd2 = String.format("T%d 0 %d 0\n", file.mtime(), file.atime());
                if (this.verbose) {
                    System.out.print("sending command: '" + cmd2.substring(0, cmd2.length() - 1) + "\\n' ... ");
                }
                _cout.write(cmd2.getBytes());
                _cout.flush();
                recvAck(_cin);
                if (this.verbose) {
                    System.out.println("done");
                }
            }
            // send file mode, length and name
            String cmd3 = String.format("C%04o %d %s\n", file.mode(), file.length(), file.name());
            if (this.verbose) {
                System.out.print("sending command: '" + cmd3.substring(0, cmd3.length() - 1) + "\\n' ... ");
            }
            _cout.write(cmd3.getBytes(encoding()));
            _cout.flush();
            recvAck(_cin);
            if (this.verbose) {
                System.out.println("done");
            }

            if (this.verbose) {
                System.out.print("sending file content ... ");
            }
            // send file content
            StreamUtils.copy(in, file.length(), _cout);
            sendAck(_cout);
            recvAck(_cin);
            if (this.verbose) {
                System.out.println("done");
            }
        }
        if (parentComponents != null) {
            for (int i = 0; i < parentComponents.length; i++) {
                sendPopDir();
            }
        }
    }

    private void initializePut() throws Throwable {

        if (_initializedPut) {
            return;
        }
        // @formatter:off
        // -r    recursive 
        // -d    target should be a directory
        // -t    ???
        // -p    preserve mtime
        // -C    compress
        // @formatter:on
        StringBuilder sb = new StringBuilder("scp -r -d -t ");
        if (preserve()) {
            sb.append("-p ");
        }
        if (compress()) {
            sb.append("-C ");
        }
        if (remoteBaseDirectory() == null || remoteBaseDirectory().isEmpty()) {
            sb.append(Connection.DEFAULT_REMOTE_BASE_DIRECTORY);
        } else {
            sb.append(remoteBaseDirectory());
        }
        String cmd = sb.toString();
        this.channel.setCommand(cmd);
        if (this.verbose) {
            System.out.print("open exec channel by sending command: '" + cmd + "' ... ");
        }
        this.channel.connect();
        recvAck(_cin);
        if (this.verbose) {
            System.out.println("done");
        }
        _initializedPut = true;
    }

    /**
     * Enter the specified sub-directory.
     * 
     * @param dirName
     *            The name of the sub-directory.
     * @throws IOException
     */
    private void sendPushDir(String dirName) throws IOException {
        String cmd = String.format("D%04o 0 %s\n", defaultDirectoryMode(), dirName);
        if (this.verbose) {
            System.out.print("sending command: '" + cmd.substring(0, cmd.length() - 1) + "\\n' ... ");
        }
        _cout.write(cmd.getBytes(encoding()));
        _cout.flush();
        recvAck(_cin);
        if (this.verbose) {
            System.out.println("done");
        }
    }

    /**
     * Go back to parent directory. (cd ..)
     * 
     * @throws IOException
     */
    private void sendPopDir() throws IOException {
        String cmd = "E\n";
        if (this.verbose) {
            System.out.print("sending command: '" + cmd.substring(0, cmd.length() - 1) + "\\n' ... ");
        }
        _cout.write(cmd.getBytes(encoding()));
        _cout.flush();
        recvAck(_cin);
        if (this.verbose) {
            System.out.println("done");
        }
    }

    @Override
    public void get(String remotePath, GetHandler h) throws Throwable {
        if (remoteBaseDirectory() != null && !remoteBaseDirectory().isEmpty()) {
            remotePath = PathUtils.join(remoteBaseDirectory(), remotePath);
        }

        /*
         * send initial command
         */

        // @formatter:off
        // -r    recursive 
        // -p    preserve permissions, mtime and atime
        // @formatter:on
        StringBuilder sb = new StringBuilder("scp -f ");
        sb.append("-r ");
        if (preserve()) {
            sb.append("-p ");
        }
        sb.append(remotePath);
        String cmd = sb.toString();
        this.channel.setCommand(cmd);
        if (this.verbose) {
            System.out.print("open exec channel by sending command: '" + cmd + "' ... ");
        }
        this.channel.connect();
        if (this.verbose) {
            System.out.println("done");
        }

        /*
         * receive directories and files
         */
        Stack<String> parents = new Stack<String>();
        String currentDir = null;
        while (!this.channel.isClosed()) {
            sendAck(_cout);
            String msg = StreamUtils.readLine(_cin, encoding());
            if (msg == null) {
                break;
            }
            if (this.verbose) {
                System.out.println("Received message: '" + msg + "'");
            }
            switch (msg.charAt(0)) {
            case 'C':
                FileAttrs file = parseFileAttrs(msg, currentDir);
                if (this.verbose) {
                    System.out.print("Receiving file: '" + file.path() + "' ... ");
                }
                sendAck(_cout);
                h.getFile(file, new SizedInputStream(_cin, file.length(), false));
                recvAck(_cin);
                if (this.verbose) {
                    System.out.println("done");
                }
                break;
            case 'T':
                int[] times = parseTimeAttrs(msg);
                sendAck(_cout);
                msg = StreamUtils.readLine(_cin);
                FileAttrs attrs;
                char c = msg.charAt(0);
                if (c == 'C') {
                    attrs = parseFileAttrs(msg, currentDir);
                    attrs.setMTime(times[0]);
                    attrs.setATime(times[1]);
                    if (this.verbose) {
                        System.out.print("Receiving file: '" + attrs.path() + "' ... ");
                    }
                    sendAck(_cout);
                    h.getFile(attrs, new SizedInputStream(_cin, attrs.length(), false));
                    recvAck(_cin);
                    if (this.verbose) {
                        System.out.println("done");
                    }
                } else if (c == 'D') {
                    attrs = parseDirAttrs(msg, currentDir);
                    attrs.setMTime(times[0]);
                    attrs.setATime(times[1]);
                    if (this.verbose) {
                        System.out.print("Receiving directory: '" + attrs.path() + "' ... ");
                    }
                    h.getDirectory(attrs);
                    parents.push(attrs.name());
                    currentDir = attrs.path();
                    if (this.verbose) {
                        System.out.println("done");
                    }
                } else {
                    throw new IOException("Unexpected response: " + msg);
                }
                break;
            case 'D':
                FileAttrs dir = parseDirAttrs(msg, currentDir);
                if (this.verbose) {
                    System.out.print("Receiving directory: '" + dir.path() + "' ... ");
                }
                h.getDirectory(dir);
                parents.push(dir.name());
                currentDir = dir.path();
                if (this.verbose) {
                    System.out.println("done");
                }
                break;
            case 'E':
                if (this.verbose) {
                    System.out.print("Going back to parent directory ... ");
                }
                parents.pop();
                currentDir = PathUtils.join(parents);
                if (this.verbose) {
                    System.out.println("done");
                }
                break;
            case '\01':
            case '\02':
                throw new IOException(msg.substring(1));
            default:
                throw new IOException(msg);
            }
        }

    }

    private int[] parseTimeAttrs(String msg) {
        // T{mtime} 0 {atime} 0
        String[] times = msg.substring(1).split(" ");
        int mtime = Integer.parseInt(times[0]);
        int atime = times.length < 3 ? mtime : Integer.parseInt(times[2]);
        return new int[] { mtime, atime };
    }

    private FileAttrs parseFileAttrs(String msg, String currentDir) {
        // C{mode} {length} {file_name}
        String[] parts = msg.substring(1).split(" ");
        int mode = Integer.parseInt(parts[0], 8);
        long length = Long.parseLong(parts[1]);
        String filename = parts[2];
        return new FileAttrs(PathUtils.join(currentDir, filename), mode, length, null, null);
    }

    private FileAttrs parseDirAttrs(String msg, String currentDir) {
        // D{mode} 0 {dir_name}
        String[] parts = msg.substring(1).split(" ");
        int mode = Integer.parseInt(parts[0], 8);
        String dirname = parts[2];
        return new FileAttrs(currentDir == null ? dirname : PathUtils.join(currentDir, dirname), mode, null, null);
    }

    @Override
    public final String channelType() {
        return Executor.CHANNEL_TYPE_NAME;
    }

    private static void recvAck(InputStream in) throws IOException {
        final int b = in.read();
        // b may be
        // 0 for success,
        // 1 for error,
        // 2 for fatal error,

        if (b == -1) {
            // didn't receive any response
            throw new IOException("No response from remote server.");
        } else if (b != 0) {
            StringBuffer sb = new StringBuffer();
            int c = in.read();
            while (c > 0 && c != '\n') {
                sb.append((char) c);
                c = in.read();
            }
            if (b == 1) {
                throw new IOException("Remote server responded error(" + b + "): " + sb.toString());
            } else if (b == 2) {
                throw new IOException("Remote server responded fatal error(" + b + "): " + sb.toString());
            } else {
                throw new IOException("Remote server responded unknown error(" + b + "): " + sb.toString());
            }
        }
    }

    private static void sendAck(OutputStream out) throws IOException {
        out.write(new byte[] { 0 });
        out.flush();
    }

    public static void main(String[] args) throws Throwable {

        String[] parts = PathUtils.trimSlash("/a/b/c/").split("/+");
        int i = 0;
        for (String part : parts) {
            System.out.println("" + (i++) + ": " + part);
        }
    }
}
