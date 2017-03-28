package daris.ssh.plugin.sink;

import java.io.InputStream;
import java.util.Map;

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
import daris.ssh.client.SSHClient;
import daris.ssh.client.SSHClientBuilder;
import daris.util.PathUtils;

public abstract class SSHSink extends AbstractDataSink {

    public static final String PARAM_HOST = "host";
    public static final String PARAM_PORT = "port";
    public static final String PARAM_USERNAME = "username";
    public static final String PARAM_PASSWORD = "password";
    public static final String PARAM_PRIVATE_KEY = "private-key";
    public static final String PARAM_DIRECTORY = "directory";
    public static final String PARAM_UNARCHIVE = "unarchive";

    protected SSHSink(String typeName) throws Throwable {
        super(typeName);

        /*
         * init param definitions
         */
        addParameterDefinition(PARAM_HOST, StringType.DEFAULT, "SSH server host.");
        addParameterDefinition(PARAM_PORT, new IntegerType(1, 65535), "SSH server port.");
        addParameterDefinition(PARAM_USERNAME, StringType.DEFAULT, "SSH username.");
        addParameterDefinition(PARAM_PASSWORD, PasswordType.DEFAULT, "User's password.");
        addParameterDefinition(PARAM_PRIVATE_KEY, PasswordType.DEFAULT, "User's private key.");
        addParameterDefinition(PARAM_DIRECTORY, StringType.DEFAULT,
                "The default/base directory on the remote SSH server. If not specified, defaults to user's home directory.");
        addParameterDefinition(PARAM_UNARCHIVE, BooleanType.DEFAULT, "Extract archive contents. Defaults to false.");
    }

    public String[] acceptedTypes() throws Throwable {
        return null;
    }

    public Object beginMultiple(Map<String, String> params) throws Throwable {
        validateParams(params);
        return getClient(params);
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
        String directory = params.get(PARAM_DIRECTORY);
        String assetSpecificOutputPath = multiTransferContext != null ? null : getAssetSpecificOutput(params);
        boolean unarchive = false;
        if (params.containsKey(PARAM_UNARCHIVE)) {
            try {
                unarchive = Boolean.parseBoolean(params.get(PARAM_UNARCHIVE));
            } catch (Throwable e) {
                unarchive = false;
            }
        }
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
        SSHClient client = null;
        try {
            client = getClient(multiTransferContext, params);
            if (unarchive) {
                String dirPath = OutputPath.getOutputPath(directory, assetSpecificOutputPath, path, assetMeta, true);
                ArchiveInput ai = ArchiveRegistry.createInput(in, new NamedMimeType(mimeType));
                ArchiveInput.Entry entry;
                try {
                    while ((entry = ai.next()) != null) {
                        if (entry.isDirectory()) {
                            client.mkdirs(PathUtils.join(dirPath, entry.name()));
                        } else {
                            put(client, entry.stream(), entry.size(), PathUtils.join(dirPath, entry.name()));
                        }
                    }
                } finally {
                    ai.close();
                }
            } else {
                String remoteFilePath = OutputPath.getOutputPath(directory, assetSpecificOutputPath, path, assetMeta,
                        false);
                put(client, in, length, remoteFilePath);
            }
        } finally {
            if (multiTransferContext == null && client != null) {
                client.close();
            }
        }
    }

    protected abstract void put(SSHClient client, InputStream in, long length, String remoteFilePath) throws Throwable;

    public void endMultiple(Object multiTransferContext) throws Throwable {
        if (multiTransferContext != null) {
            SSHClient client = (SSHClient) multiTransferContext;
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

    protected SSHClient getClient(Map<String, String> params) throws Throwable {
        SSHClientBuilder builder = SSHClientBuilder.getInstance();
        builder.setHost(params.get(PARAM_HOST));
        builder.setPort(Integer.parseInt(params.getOrDefault(PARAM_PORT, "22")));
        builder.setUsername(params.get(PARAM_USERNAME));
        builder.setPassword(params.get(PARAM_PASSWORD));
        builder.setPrivateKey(params.get(PARAM_PRIVATE_KEY), null);
        return builder.build();
    }

    private SSHClient getClient(Object multiTransferContext, Map<String, String> params) throws Throwable {
        if (multiTransferContext != null) {
            return (SSHClient) multiTransferContext;
        } else {
            return getClient(params);
        }
    }

}
