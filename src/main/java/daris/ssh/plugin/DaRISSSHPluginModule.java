package daris.ssh.plugin;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import arc.mf.plugin.ConfigurationResolver;
import arc.mf.plugin.DataSinkRegistry;
import arc.mf.plugin.PluginModule;
import arc.mf.plugin.PluginService;
import daris.ssh.plugin.services.SvcHostKeyScan;
import daris.ssh.plugin.services.SvcScpGet;
import daris.ssh.plugin.services.SvcScpPut;
import daris.ssh.plugin.services.SvcSftpGet;
import daris.ssh.plugin.services.SvcSftpPut;
import daris.ssh.plugin.sink.ScpSink;
import daris.ssh.plugin.sink.SftpSink;

public class DaRISSSHPluginModule implements PluginModule {

    private List<PluginService> _services;

    private ScpSink _scpSink;
    private SftpSink _sftpSink;

    public DaRISSSHPluginModule() {
        _services = new ArrayList<PluginService>();
        _services.add(new SvcHostKeyScan());
        _services.add(new SvcSftpGet());
        _services.add(new SvcSftpPut());
        _services.add(new SvcScpGet());
        _services.add(new SvcScpPut());
    }

    public String description() {
        return "Plugin sinks to access remove SSH/SFTP server via scp or sftp.";
    }

    public void initialize(ConfigurationResolver conf) throws Throwable {
        try {
            if (_scpSink == null) {
                _scpSink = new ScpSink();
                DataSinkRegistry.add(this, _scpSink);
            }
            if (_sftpSink == null) {
                _sftpSink = new SftpSink();
                DataSinkRegistry.add(this, _sftpSink);
            }
        } catch (Throwable e) {
            e.printStackTrace(System.out);
            throw e;
        }
    }

    public Collection<PluginService> services() {
        return _services;
    }

    public void shutdown(ConfigurationResolver conf) throws Throwable {
        try {
            DataSinkRegistry.removeAll(this);
        } catch (Throwable e) {
            e.printStackTrace(System.out);
            throw e;
        }
    }

    public String vendor() {
        return "Research Platforms, The University of Melbourne";
    }

    public String version() {
        return "1.0.0";
    }

}
