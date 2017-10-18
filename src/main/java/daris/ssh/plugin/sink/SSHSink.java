package daris.ssh.plugin.sink;

import java.io.Closeable;
import java.io.InputStream;
import java.util.Map;
import java.util.regex.Pattern;

import arc.archive.ArchiveInput;
import arc.archive.ArchiveRegistry;
import arc.mf.plugin.dtype.BooleanType;
import arc.mf.plugin.dtype.IntegerType;
import arc.mf.plugin.dtype.PasswordType;
import arc.mf.plugin.dtype.StringType;
import arc.mime.NamedMimeType;
import arc.streams.LongInputStream;
import arc.xml.XmlDoc.Element;
import daris.plugin.sink.AbstractDataSink;
import daris.plugin.sink.util.OutputPath;
import io.github.xtman.ssh.util.PathUtils;

public abstract class SSHSink extends AbstractDataSink {

    static interface Client extends Closeable {

        /**
         * The remote base directory.
         * 
         * @return
         */
        String baseDirectory();

        /**
         * Put the input (file) stream to the remote destination.
         * 
         * @param in
         *            The input stream.
         * @param length
         *            Length of the input.
         * @param dstPath
         *            The destination path relative to the base directory.
         * @throws Throwable
         */
        void put(InputStream in, long length, String dstPath) throws Throwable;

        /**
         * Make directories recursively.
         * 
         * @param dstDirPath
         *            The destination directory path relative to the base
         *            directory.
         * @throws Throwable
         */
        void mkdirs(String dstDirPath) throws Throwable;

    }

    public static final int DEFAULT_FILE_MODE = 0640;

    public static final int DEFAULT_DIR_MODE = 0755;

    public static final String PARAM_HOST = "host";
    public static final String PARAM_PORT = "port";
    public static final String PARAM_HOST_KEY = "host-key";
    public static final String PARAM_USERNAME = "username";
    public static final String PARAM_PASSWORD = "password";
    public static final String PARAM_PRIVATE_KEY = "private-key";
    public static final String PARAM_PASSPHRASE = "passphrase";
    public static final String PARAM_DIRECTORY = "directory";
    public static final String PARAM_UNARCHIVE = "unarchive";
    public static final String PARAM_DIR_MODE = "dir-mode";
    public static final String PARAM_FILE_MODE = "file-mode";

    protected SSHSink(String typeName) throws Throwable {
        super(typeName);

        /*
         * init param definitions
         */
        addParameterDefinition(PARAM_HOST, StringType.DEFAULT, "SSH server host.");
        addParameterDefinition(PARAM_PORT, new IntegerType(1, 65535), "SSH server port.");
        addParameterDefinition(PARAM_HOST_KEY, StringType.DEFAULT,
                "SSH server host public key. If specified, host key will be validated.");
        addParameterDefinition(PARAM_USERNAME, StringType.DEFAULT, "SSH username.");
        addParameterDefinition(PARAM_PASSWORD, PasswordType.DEFAULT, "User's password.");
        addParameterDefinition(PARAM_PRIVATE_KEY, PasswordType.DEFAULT, "User's private key.");
        addParameterDefinition(PARAM_PASSPHRASE, PasswordType.DEFAULT, "Passphrase for user's private key.");
        addParameterDefinition(PARAM_DIRECTORY, StringType.DEFAULT,
                "The default/base directory on the remote SSH server. If not specified, defaults to user's home directory.");
        addParameterDefinition(PARAM_UNARCHIVE, BooleanType.DEFAULT, "Extract archive contents. Defaults to false.");
        addParameterDefinition(PARAM_DIR_MODE, new StringType(Pattern.compile("^[0-7]{4}$")),
                "Remote directory mode (permissions). Defaults to " + String.format("%04o", DEFAULT_DIR_MODE));
        addParameterDefinition(PARAM_FILE_MODE, new StringType(Pattern.compile("^[0-7]{4}$")),
                "Remote file mode (permissions). Defaults to " + String.format("%04o", DEFAULT_FILE_MODE));
    }

    public String[] acceptedTypes() throws Throwable {
        return null;
    }

    public Object beginMultiple(Map<String, String> params) throws Throwable {
        validateParams(params);
        return createClient(params);
    }

    public int compressionLevelRequired() {
        // don't care
        return -1;
    }

    public void consume(Object multiTransferContext, String path, Map<String, String> params, Element userMeta,
            Element assetMeta, LongInputStream in, String appMimeType, String streamMimeType, long length)
            throws Throwable {
        if (multiTransferContext == null) {
            // if it is in multi transfer context, params were already validated
            // in beginMultiple() method.
            validateParams(params);
        }
        String assetSpecificOutputPath = multiTransferContext != null ? null : getAssetSpecificOutput(params);
        boolean unarchive = Boolean.parseBoolean(params.getOrDefault(PARAM_UNARCHIVE, "false"));
        String mimeType = streamMimeType;
        if (mimeType == null && assetMeta != null) {
            mimeType = assetMeta.value("content/type");
        }
        if (!ArchiveRegistry.isAnArchive(mimeType) && unarchive) {
            unarchive = false;
        }
        /*
         * 
         */
        Client client = null;
        try {
            client = getOrCreateClient(multiTransferContext, params);
            if (unarchive) {
                String dirPath = OutputPath.getOutputPath(null, assetSpecificOutputPath, path, assetMeta, true);
                ArchiveInput ai = ArchiveRegistry.createInput(in, new NamedMimeType(mimeType));
                ArchiveInput.Entry entry;
                try {
                    while ((entry = ai.next()) != null) {
                        try {
                            if (entry.isDirectory()) {
                                client.mkdirs(PathUtils.join(dirPath, entry.name()));
                            } else {
                                client.put(entry.stream(), entry.size(), PathUtils.join(dirPath, entry.name()));
                            }
                        } finally {
                            ai.closeEntry();
                        }
                    }
                } finally {
                    ai.close();
                }
            } else {
                String dstPath = OutputPath.getOutputPath(null, assetSpecificOutputPath, path, assetMeta, false);
                client.put(in, length, dstPath);
            }
        } finally {
            if (multiTransferContext == null && client != null) {
                client.close();
            }
        }
    }

    // protected abstract void put(SSHClient client, InputStream in, long
    // length, String remoteFilePath) throws Throwable;

    public void endMultiple(Object multiTransferContext) throws Throwable {
        if (multiTransferContext != null) {
            Client client = (Client) multiTransferContext;
            client.close();
        }
    }

    public void shutdown() throws Throwable {

    }

    protected void validateParams(Map<String, String> params) {
        if (!params.containsKey(PARAM_HOST)) {
            throw new IllegalArgumentException("Missing host argument.");
        }
        if (!params.containsKey(PARAM_USERNAME)) {
            throw new IllegalArgumentException("Missing username argument.");
        }
        if (!params.containsKey(PARAM_PASSWORD) && !params.containsKey(PARAM_PRIVATE_KEY)) {
            throw new IllegalArgumentException("Missing password argument and/or private-key argument.");
        }
    }

    private Client createClient(Map<String, String> params) throws Throwable {
        String host = params.get(PARAM_HOST);
        int port = Integer.parseInt(params.getOrDefault(PARAM_PORT, "22"));
        String hostKey = params.get(PARAM_HOST_KEY);
        String username = params.get(PARAM_USERNAME);
        String password = params.get(PARAM_PASSWORD);
        String privateKey = params.get(PARAM_PRIVATE_KEY);
        String passphrase = params.get(PARAM_PASSPHRASE);
        String directory = params.get(PARAM_DIRECTORY);
        int dirMode = Integer.parseInt(params.getOrDefault(PARAM_DIR_MODE, String.format("%04o", DEFAULT_DIR_MODE)), 8);
        int fileMode = Integer.parseInt(params.getOrDefault(PARAM_FILE_MODE, String.format("%04o", DEFAULT_FILE_MODE)),
                8);
        return createClient(host, port, hostKey, username, password, privateKey, passphrase, directory, dirMode,
                fileMode);
    }

    protected abstract Client createClient(String host, int port, String hostKey, String username, String password,
            String privateKey, String passphrase, String directory, int dirMode, int fileMode) throws Throwable;

    private Client getOrCreateClient(Object multiTransferContext, Map<String, String> params) throws Throwable {
        if (multiTransferContext != null) {
            return (Client) multiTransferContext;
        } else {
            return createClient(params);
        }
    }

}
