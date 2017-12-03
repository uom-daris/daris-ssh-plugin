package io.github.xtman.ssh.client.cli;

import java.io.Console;
import java.io.FileNotFoundException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import io.github.xtman.ssh.client.Connection;
import io.github.xtman.ssh.client.ConnectionBuilder;
import io.github.xtman.ssh.client.ScpClient;

public class ScpCLI {

    public static final String APP = "scp";

    public static void main(String[] args) throws Throwable {
        try {

            long startTime = System.currentTimeMillis();

            if (args.length < 2) {
                throw new IllegalArgumentException("Missing arguments.");
            }

            String[] dst = parseDestination(args[args.length - 1]);
            String user = dst[0];
            String host = dst[1];
            int port = 22;
            String baseDir = dst[2];
            String password = null;

            List<Path> inputs = new ArrayList<Path>();
            for (int i = 0; i < args.length - 1;) {
                if ("-P".equals(args[i])) {
                    String p = args[i + 1];
                    try {
                        port = Integer.parseInt(args[i + 1]);
                    } catch (NumberFormatException e) {
                        throw new IllegalArgumentException("Invalid port: " + p);
                    }
                    i += 2;
                } else if ("--password".equals(args[i])) {
                    password = args[i + 1];
                    i += 2;
                } else {
                    Path input = Paths.get(args[i]);
                    if (!Files.exists(input)) {
                        throw new IllegalArgumentException(
                                new FileNotFoundException("Input file/directory " + input + " does not exist."));
                    }
                    inputs.add(input);
                    i++;
                }
            }
            if (user == null) {
                user = System.getProperty("user.name");
            }

            if (user == null) {
                user = readUsernameFromConsole();
            }

            if (password == null) {
                password = readPasswordFromConsole();
            }

            ConnectionBuilder builder = new ConnectionBuilder();
            builder.setImplementation("jsch");
            builder.setServer(host, port, null).setUserCredentials(user, password);
            Connection cxn = builder.build();
            try {
                ScpClient scp = cxn.createScpClient(baseDir, "UTF-8", 0755, 0644, false, false);
                try {
                    for (Path input : inputs) {
                        if (Files.isDirectory(input)) {
                            scp.putDirectory(input);
                        } else {
                            scp.put(input, input.getFileName().toString());
                        }
                    }
                } finally {
                    scp.close();
                }
            } finally {
                cxn.close();
            }
            long endTime = System.currentTimeMillis();
            System.out.println(String.format("%32s    %8d ms", "elapsed (milliseconds): ", endTime - startTime));
        } catch (IllegalArgumentException iae) {
            System.err.println("Error: " + iae.getMessage());
            printUsage();
        }
    }

    private static void printUsage() {
        System.out.println("");
        System.out.println("Usage: " + APP + " [-P port] <file|dir> [user@]host:dir");
        System.out.println("");
        System.out.println("Examples:");
        System.out.println("    " + APP + " ~/file1.txt spartan.hpc.unimelb.edu.au:test");
        System.out.println("    " + APP + " ~/dir1/ spartan.hpc.unimelb.edu.au:test");
        System.out.println("");
    }

    private static String[] parseDestination(String dst) {
        String[] r = new String[3];
        dst = dst.trim();
        int idx1 = dst.indexOf('@');
        if (idx1 >= 0) {
            r[0] = idx1 == 0 ? null : dst.substring(0, idx1);
            dst = dst.substring(idx1 + 1);
        } else {
            r[0] = null;
        }
        int idx2 = dst.indexOf(':');
        if (idx2 >= 0) {
            r[1] = dst.substring(0, idx2);
            r[2] = idx2 == dst.length() - 1 ? null : dst.substring(idx2 + 1);
        } else {
            r[1] = dst;
            r[2] = null;
        }
        return r;
    }

    private static String readUsernameFromConsole() {
        String user = null;
        Scanner scanner = new Scanner(System.in);
        try {
            while (user == null || user.trim().isEmpty()) {
                System.out.print("login: ");
                user = scanner.nextLine().trim();
            }
        } finally {
            scanner.close();
        }
        return user;
    }

    private static String readPasswordFromConsole() {
        Console console = System.console();
        String password = null;
        while (password == null || password.trim().isEmpty()) {
            password = new String(console.readPassword("password: "));
        }
        return password;
        //@formatter:off
//        String password = null;
//        Scanner scanner = new Scanner(System.in);
//        try {
//            while (password == null || password.trim().isEmpty()) {
//                System.out.print("password: ");
//                password = scanner.nextLine().trim();
//            }
//        } finally {
//            scanner.close();
//        }
//        return password;
        //@formatter:on
    }

}
