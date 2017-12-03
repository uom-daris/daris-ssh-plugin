package daris.ssh.plugin.services;

import arc.mf.plugin.PluginService;
import arc.mf.plugin.ServiceExecutor;
import arc.mf.plugin.dtype.EnumType;
import arc.mf.plugin.dtype.IntegerType;
import arc.mf.plugin.dtype.PasswordType;
import arc.mf.plugin.dtype.StringType;
import arc.xml.XmlDoc;
import arc.xml.XmlWriter;
import io.github.xtman.ssh.client.Connection;
import io.github.xtman.ssh.client.ConnectionBuilder;

public abstract class AbstractSshService extends PluginService {

    protected Interface defn;

    AbstractSshService() {
        this.defn = new Interface();
        this.defn.add(new Interface.Element("host", StringType.DEFAULT, "SSH server host address.", 1, 1));
        this.defn.add(new Interface.Element("port", new IntegerType(0, 65535), "SSH server port. Defaults 22.", 0, 1));
        this.defn.add(new Interface.Element("user", StringType.DEFAULT, "SSH user name.", 1, 1));
        Interface.Element password = new Interface.Element("password", PasswordType.DEFAULT, "SSH user's password.", 0,
                1);
        password.add(new Interface.Attribute("type", new EnumType(new String[] { "value", "reference" }),
                "value or reference to a secure wallet entry. If the latter, key for the entry is specified. Defaults to value.",
                0));
        this.defn.add(password);
        Interface.Element privateKey = new Interface.Element("private-key", PasswordType.DEFAULT,
                "SSH user's private key.", 0, 1);
        privateKey.add(new Interface.Attribute("passphrase", PasswordType.DEFAULT,
                "Passphrase to decode the private-key.", 0));
        privateKey.add(new Interface.Attribute("type", new EnumType(new String[] { "value", "reference" }),
                "value or reference to a secure wallet entry. If the latter, key for the entry is specified. Defaults to value.",
                0));
        this.defn.add(privateKey);
    }

    @Override
    public Interface definition() {
        return this.defn;
    }

    @Override
    public void execute(XmlDoc.Element args, Inputs inputs, Outputs outputs, XmlWriter w) throws Throwable {
        ConnectionBuilder cb = new ConnectionBuilder();
        cb.setHost(args.value("host"));
        cb.setPort(args.intValue("port", 22));
        cb.setUsername(args.value("user"));
        if (args.elementExists("password")) {
            String type = args.stringValue("@type", "value");
            if ("value".equals(type)) {
                cb.setPassword(args.value("password"));
            } else {
                cb.setPassword(resolveSecureWalletEntry(executor(), args.value("password")));
            }
        } else if (args.elementExists("private-key")) {
            cb.setPassphrase(args.value("private-key/@passphrase"));
            String type = args.stringValue("@type", "value");
            if ("value".equals(type)) {
                cb.setPassword(args.value("private-key"));
            } else {
                cb.setPassword(resolveSecureWalletEntry(executor(), args.value("private-key")));
            }
        } else {
            throw new IllegalArgumentException("Either password or private-key must be specified.");
        }
        Connection cxn = cb.build();
        try {
            execute(cxn, args, inputs, outputs, w);
        } finally {
            cxn.close();
        }
    }

    protected abstract void execute(Connection cxn, XmlDoc.Element args, Inputs inputs, Outputs outputs, XmlWriter w)
            throws Throwable;

    static String resolveSecureWalletEntry(ServiceExecutor executor, String key) throws Throwable {
        // TODO
        return null;
    }

}
