package daris.ssh.plugin.services;

import java.util.Base64;

import arc.mf.plugin.PluginService;
import arc.mf.plugin.dtype.IntegerType;
import arc.mf.plugin.dtype.StringType;
import arc.xml.XmlDoc.Element;
import arc.xml.XmlWriter;
import daris.ssh.client.KeyTools;
import daris.ssh.client.ganymed.GanymedClient;

public class SvcHostKeyScan extends PluginService {

    public static final String SERVICE_NAME = "daris.ssh.hostkey.scan";

    private Interface _defn;

    public SvcHostKeyScan() {
        _defn = new Interface();
        _defn.add(new Interface.Element("host", StringType.DEFAULT, "The host name or ip address of the SSH server.", 1,
                1));
        _defn.add(
                new Interface.Element("port", new IntegerType(1, 65535), "The SSH server port. Defaults to 22.", 0, 1));

    }

    @Override
    public Access access() {
        return ACCESS_ACCESS;
    }

    @Override
    public Interface definition() {
        return _defn;
    }

    @Override
    public String description() {
        return "Scan the public key of the remote SSH server.";
    }

    @Override
    public void execute(Element args, Inputs arg1, Outputs arg2, XmlWriter w) throws Throwable {
        String host = args.value("host");
        int port = args.intValue("port", 22);
        byte[] pubkey = GanymedClient.getServerHostKey(host, port);
        String type = KeyTools.getPublicKeyType(pubkey);
        w.add("public-key", new String[] { "type", type }, Base64.getEncoder().encodeToString(pubkey));
    }

    @Override
    public String name() {
        return SERVICE_NAME;
    }

}
