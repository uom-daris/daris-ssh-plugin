package daris.ssh.client.test;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Base64;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

public class TestUtils {

    public static final String TEST_PROPERTIES_FILE = System.getProperty("user.home")
            + "/.daris-ssh-plugin-test.properties";

    static Map<String, String> getTestSettings() throws Throwable {
        Map<String, String> settings = new HashMap<String, String>();
        Properties properties = new Properties();
        InputStream in = null;
        try {
            in = new BufferedInputStream(new FileInputStream(TEST_PROPERTIES_FILE));
            properties.load(in);
        } finally {
            if (in != null) {
                in.close();
            }
        }
        if (properties != null) {
            settings.put("host", properties.getProperty("host"));
            settings.put("port", properties.getProperty("port"));
            settings.put("username", properties.getProperty("username"));
            settings.put("password", new String(Base64.getDecoder().decode(properties.getProperty("password"))));
        }
        return settings;
    }

    public static File generateTempFile(long length) throws Throwable {
        String filePath = generateTempFilePath();
        File file = new File(filePath);
        OutputStream out = new BufferedOutputStream(new FileOutputStream(file));
        byte[] buffer = new byte[1024];
        Random random = new Random();
        try {
            long remaining = length;
            while (remaining > 0) {
                random.nextBytes(buffer);
                if (remaining >= buffer.length) {
                    out.write(buffer);
                    remaining -= buffer.length;
                } else {
                    out.write(buffer, 0, (int) remaining);
                    remaining = 0;
                }
            }
            out.flush();
            return file;
        } finally {
            out.close();
        }
    }

    public static String generateTempFilePath(String fileName) {
        return System.getProperty("java.io.tmpdir") + "/" + fileName;
    }

    public static String generateTempFileName() {
        return "tmp-file-" + new Date().getTime();
    }

    public static String generateTempFilePath() {
        return generateTempFilePath(generateTempFileName());
    }

    public static File createTempFile() {
        return new File(generateTempFilePath());
    }

}
