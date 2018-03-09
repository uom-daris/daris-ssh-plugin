package daris.ssh.plugin.services;

import arc.mf.plugin.PluginService;
import arc.mf.plugin.ServiceExecutor;
import arc.mf.plugin.dtype.EnumType;
import arc.mf.plugin.dtype.IntegerType;
import arc.mf.plugin.dtype.PasswordType;
import arc.mf.plugin.dtype.StringType;
import arc.xml.XmlDoc;
import arc.xml.XmlDocMaker;
import arc.xml.XmlWriter;
import io.github.xtman.ssh.client.Connection;
import io.github.xtman.ssh.client.ConnectionBuilder;

public abstract class AbstractSshService extends PluginService {

    public static class OnError {

        public static enum Action {
            CONTINUE, STOP
        }

        private Action _action;
        private int _retry;

        public OnError(XmlDoc.Element oe) throws Throwable {
            if (oe == null) {
                _action = DEFAULT_ACTION_ON_ERROR;
                _retry = 0;
            } else {
                _action = oe.value() == null ? DEFAULT_ACTION_ON_ERROR : Action.valueOf(oe.value().toUpperCase());
                _retry = oe.intValue("@retry", 0);
            }
        }

        public OnError(Action action, int retry) {
            _action = action;
            _retry = retry;
        }

        public Action action() {
            return _action;
        }

        public int retry() {
            return _retry;
        }

        public boolean stopOnError() {
            return Action.STOP.equals(_action);
        }

        public boolean continueOnError() {
            return Action.CONTINUE.equals(_action);
        }

    }

    public static final OnError.Action DEFAULT_ACTION_ON_ERROR = OnError.Action.STOP;

    public static final String SECURE_WALLET_KEY_XPATH_SEPARATOR = "::";

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
        privateKey.add(new Interface.Attribute("type", new EnumType(new String[] { "value", "reference" }),
                "value or reference to a secure wallet entry. If the latter, key for the entry is specified. Defaults to value.",
                0));
        this.defn.add(privateKey);

        Interface.Element passphrase = new Interface.Element("passphrase", PasswordType.DEFAULT,
                "Passphrase for the private key. Ignored if no private-key is specified.", 0, 1);
        passphrase.add(new Interface.Attribute("type", new EnumType(new String[] { "value", "reference" }),
                "value or reference to a secure wallet entry. If the latter, key for the entry is specified. Defaults to value.",
                0));
        this.defn.add(passphrase);

        Interface.Element onError = new Interface.Element("on-error", new EnumType(new String[] { "continue", "stop" }),
                "The behaviour to exhibit when encountering an error. Defaults to '"
                        + DEFAULT_ACTION_ON_ERROR.name().toLowerCase() + "'.",
                0, 1);
        onError.add(
                new Interface.Attribute("retry", IntegerType.POSITIVE, "Number of retry attempts. Defaults to 0", 0));
        this.defn.add(onError);

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
            String type = args.stringValue("password/@type", "value");
            if ("value".equals(type)) {
                cb.setPassword(args.value("password"));
            } else {
                cb.setPassword(resolveSecureWalletEntry(executor(), args.value("password")));
            }
        } else if (args.elementExists("private-key")) {
            if (args.elementExists("passphrase")) {
                String type = args.stringValue("passphrase/@type", "value");
                if ("value".equals(type)) {
                    cb.setPassphrase(args.value("passphrase"));
                } else {
                    cb.setPassphrase(resolveSecureWalletEntry(executor(), args.value("passphrase")));
                }
            }
            String type = args.stringValue("private-key/@type", "value");
            if ("value".equals(type)) {
                cb.setPassword(args.value("private-key"));
            } else {
                cb.setPassword(resolveSecureWalletEntry(executor(), args.value("private-key")));
            }
        } else {
            throw new IllegalArgumentException("Either password or private-key must be specified.");
        }
        Connection cxn = cb.build();
        OnError onError = args.elementExists("on-error") ? new OnError(args.element("on-error"))
                : new OnError(DEFAULT_ACTION_ON_ERROR, 0);
        try {
            execute(cxn, args, inputs, outputs, w, onError);
        } finally {
            cxn.close();
        }
    }

    protected abstract void execute(Connection cxn, XmlDoc.Element args, Inputs inputs, Outputs outputs, XmlWriter w,
            OnError onError) throws Throwable;

    @Override
    public boolean canBeAborted() {
        return true;
    }

    static String resolveSecureWalletEntry(ServiceExecutor executor, String key) throws Throwable {
        int idx = key.indexOf(SECURE_WALLET_KEY_XPATH_SEPARATOR);
        String k = idx == -1 ? key : key.substring(0, idx);
        String xpath = idx == -1 ? null : key.substring(idx + SECURE_WALLET_KEY_XPATH_SEPARATOR.length());

        XmlDocMaker dm = new XmlDocMaker("args");
        dm.add("key", k);
        XmlDoc.Element re = executor.execute("secure.wallet.get", dm.root());
        String value = null;
        if (xpath != null) {
            value = re.value(xpath);
        } else {
            value = re.value("value");
        }
        if (value == null) {
            throw new IllegalArgumentException("Failed to retrieve '" + key + "' from secure wallet.");
        }
        return value;
    }

}
