package daris.ssh.plugin.sink;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Map;
import java.util.regex.Pattern;

import arc.archive.ArchiveInput;
import arc.archive.ArchiveRegistry;
import arc.mf.plugin.PluginTask;
import arc.mf.plugin.dtype.BooleanType;
import arc.mf.plugin.dtype.IntegerType;
import arc.mf.plugin.dtype.PasswordType;
import arc.mf.plugin.dtype.StringType;
import arc.mf.plugin.sink.ParameterDefinition;
import arc.mime.NamedMimeType;
import arc.streams.LongInputStream;
import arc.streams.StreamCopy;
import arc.xml.XmlDoc.Element;
import daris.plugin.sink.AbstractDataSink;
import daris.plugin.sink.util.OutputPath;
import io.github.xtman.ssh.client.ConnectionBuilder;
import io.github.xtman.ssh.client.TransferClient;
import io.github.xtman.util.PathUtils;

public abstract class SshSink extends AbstractDataSink {

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

    protected SshSink(String typeName) throws Throwable {
        super(typeName);

    }

    @Override
    protected void addParameterDefinitions(Map<String, ParameterDefinition> paramDefns) throws Throwable {
        /*
         * init param definitions
         */
        // @formatter:off
        
        // {{                 --- start
        // }}                 --- end
        // default=DEFAULT    --- default value
        // admin              --- for admin only, should not be presented to end user
        // text               --- multiple lines text
        // optional           --- optional
        // xor=PARAM1|PARAM2  --- 
        // mutable            --- 
        // pattern=PATTERN    --- regex pattern to validate string value
        // enum=VALUE1|VALUE2 --- enumerated values
        
        // @formatter:on
        addParameterDefinition(paramDefns, PARAM_HOST, StringType.DEFAULT, "SSH server host.", false);
        addParameterDefinition(paramDefns, PARAM_PORT, new IntegerType(1, 65535), "SSH server port.{{default=22}}",
                false);
        addParameterDefinition(paramDefns, PARAM_HOST_KEY, StringType.DEFAULT,
                "SSH server host public key. If specified, host key will be validated.{{admin,text,optional}}", false);
        addParameterDefinition(paramDefns, PARAM_USERNAME, StringType.DEFAULT, "SSH username.", false);
        addParameterDefinition(paramDefns, PARAM_PASSWORD, PasswordType.DEFAULT,
                "User's password.{{optional,xor=" + PARAM_PRIVATE_KEY + "}}", false);
        addParameterDefinition(paramDefns, PARAM_PRIVATE_KEY, PasswordType.DEFAULT,
                "User's private key.{{text,optional,xor=" + PARAM_PASSWORD + "}}", false);
        addParameterDefinition(paramDefns, PARAM_PASSPHRASE, PasswordType.DEFAULT,
                "Passphrase for user's private key.{{optional}}", false);
        addParameterDefinition(paramDefns, PARAM_DIRECTORY, StringType.DEFAULT,
                "The default/base directory on the remote SSH server. If not specified, defaults to user's home directory.{{optional,mutable}}",
                false);
        addParameterDefinition(paramDefns, PARAM_UNARCHIVE, BooleanType.DEFAULT,
                "Extract archive contents. Defaults to false.{{optional,mutable,default=false}}", false);
        addParameterDefinition(paramDefns, PARAM_DIR_MODE, new StringType(Pattern.compile("^[0-7]{4}$")),
                "Remote directory mode (permissions). Defaults to " + String.format("%04o", DEFAULT_DIR_MODE)
                        + ".{{optional,mutable,pattern=^[0-7]{4}$,default=" + String.format("%04o", DEFAULT_DIR_MODE)
                        + "}}",
                false);
        addParameterDefinition(paramDefns, PARAM_FILE_MODE, new StringType(Pattern.compile("^[0-7]{4}$")),
                "Remote file mode (permissions). Defaults to " + String.format("%04o", DEFAULT_FILE_MODE)
                        + ".{{optional,mutable,pattern=^[0-7]{4}$,default=" + String.format("%04o", DEFAULT_FILE_MODE)
                        + "}}",
                false);
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
        TransferClient client = null;
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
                                long size = entry.size();
                                if (size < 0) {
                                    // entry size was not set
                                    File tf = PluginTask.createTemporaryFile();
                                    try {
                                        StreamCopy.copy(entry.stream(), tf);
                                        InputStream ti = new BufferedInputStream(new FileInputStream(tf));
                                        try {
                                            client.put(ti, tf.length(), PathUtils.join(dirPath, entry.name()));
                                        } finally {
                                            ti.close();
                                        }
                                    } finally {
                                        PluginTask.deleteTemporaryFile(tf);
                                    }
                                } else {
                                    client.put(entry.stream(), entry.size(), PathUtils.join(dirPath, entry.name()));
                                }
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
                try {
                    client.close();
                } finally {
                    client.connection().close();
                }
            }
        }
    }

    // protected abstract void put(SSHClient client, InputStream in, long
    // length, String remoteFilePath) throws Throwable;

    public void endMultiple(Object multiTransferContext) throws Throwable {
        if (multiTransferContext != null) {
            TransferClient client = (TransferClient) multiTransferContext;
            try {
                client.close();
            } finally {
                client.connection().close();
            }
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

    private TransferClient createClient(Map<String, String> params) throws Throwable {
        String directory = params.get(PARAM_DIRECTORY);
        int dirMode = Integer.parseInt(params.getOrDefault(PARAM_DIR_MODE, String.format("%04o", DEFAULT_DIR_MODE)), 8);
        int fileMode = Integer.parseInt(params.getOrDefault(PARAM_FILE_MODE, String.format("%04o", DEFAULT_FILE_MODE)),
                8);
        ConnectionBuilder cb = new ConnectionBuilder();
        cb.setHost(params.get(PARAM_HOST));
        cb.setPort(Integer.parseInt(params.getOrDefault(PARAM_PORT, "22")));
        cb.setHostKey(params.get(PARAM_HOST_KEY));
        cb.setUsername(params.get(PARAM_USERNAME));
        cb.setPassword(params.get(PARAM_PASSWORD));
        cb.setPrivateKey(params.get(PARAM_PRIVATE_KEY), params.get(PARAM_PASSPHRASE));
        return createClient(cb, directory, dirMode, fileMode);
    }

    protected abstract TransferClient createClient(ConnectionBuilder cb, String directory, int dirMode, int fileMode)
            throws Throwable;

    private TransferClient getOrCreateClient(Object multiTransferContext, Map<String, String> params) throws Throwable {
        if (multiTransferContext != null) {
            return (TransferClient) multiTransferContext;
        } else {
            return createClient(params);
        }
    }

}
