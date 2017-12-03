package io.github.xtman.io.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;

public class StreamUtils {

    public static final int BUFFER_SIZE = 8192;

    public static void copy(InputStream in, long length, OutputStream out) throws Throwable {
        copy(in, length, out, false);
    }

    public static void copy(InputStream in, long length, OutputStream out, boolean closeOutput) throws Throwable {

        byte[] buffer = new byte[BUFFER_SIZE];
        int total = 0;
        int len;
        try {
            while ((total < length || length < 0) && (len = in.read(buffer)) != -1) {
                out.write(buffer, 0, len);
                total += len;
            }
        } finally {
            if (closeOutput) {
                out.close();
            } else {
                out.flush();
            }
        }
    }

    public static void copy(InputStream in, OutputStream out, boolean closeInput, boolean closeOutput)
            throws Throwable {
        byte[] buffer = new byte[BUFFER_SIZE];
        int len;
        try {
            while ((len = in.read(buffer)) != -1) {
                out.write(buffer, 0, len);
            }
        } finally {
            try {
                if (closeOutput) {
                    out.close();
                } else {
                    out.flush();
                }
            } finally {
                if (closeInput) {
                    in.close();
                }
            }
        }
    }

    public static void copy(InputStream in, OutputStream out) throws Throwable {
        copy(in, out, false, false);
    }

    public static void copy(InputStream in, File outputFile, boolean closeInput) throws IOException {
        try {
            Files.copy(in, outputFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
        } finally {
            if (closeInput) {
                in.close();
            }
        }
    }

    public static void copy(InputStream in, File outputFile) throws IOException {
        copy(in, outputFile, false);
    }

    public static void copy(File inputFile, OutputStream out, boolean closeOutput) throws IOException {
        try {
            Files.copy(inputFile.toPath(), out);
        } finally {
            if (closeOutput) {
                out.close();
            } else {
                out.flush();
            }
        }
    }

    public static void copy(File inputFile, OutputStream out) throws IOException {
        copy(inputFile, out, false);
    }

    public static void copy(File inputFile, File outputFile) throws IOException {
        Files.copy(inputFile.toPath(), outputFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
    }

    public static void exhaustInputStream(InputStream in) throws IOException {
        if (in == null) {
            return;
        }
        try {
            byte[] buffer = new byte[BUFFER_SIZE];
            while (in.read(buffer) >= 0) {
            }
        } finally {
            in.close();
        }
    }

    public static String readLine(InputStream in, String charsetName) throws Throwable {
        BufferedReader reader = new BufferedReader(
                new InputStreamReader(new NonCloseInputStream(in), charsetName == null ? "UTF-8" : charsetName));
        try {
            return reader.readLine();
        } finally {
            reader.close();
        }
    }

    public static String readLine(InputStream in) throws Throwable {
        return readLine(in, "UTF-8");
    }

    public static String readString(InputStream in, String charsetName, boolean closeInput) throws Throwable {

        // @formatter:off
//        try {
//            ByteArrayOutputStream result = new ByteArrayOutputStream();
//            byte[] buffer = new byte[1024];
//            int length;
//            while ((length = in.read(buffer)) != -1) {
//                result.write(buffer, 0, length);
//            }
//            return result.toString(charsetName);
//        } finally {
//            if(closeInput) {
//                in.close();
//            }
//        }
        // @formatter:on

        BufferedReader reader = new BufferedReader(
                new InputStreamReader(in, charsetName == null ? "UTF-8" : charsetName));
        StringBuilder sb = new StringBuilder();
        String line = null;
        try {
            while ((line = reader.readLine()) != null) {
                sb.append(line);
            }
        } finally {
            if (closeInput) {
                try {
                    reader.close();
                } finally {
                    in.close();
                }
            }
        }
        return sb.toString();
    }

    public static String readString(InputStream in, boolean closeInput) throws Throwable {
        return readString(in, "UTF-8", closeInput);
    }
}
