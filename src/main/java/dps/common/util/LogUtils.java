package dps.common.util;

import java.io.FileWriter;
import java.io.PrintWriter;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;

/**
 * Utility for unified, thread-safe, and process-safe logging.
 * Intercepts standard print streams (System.out/err), extracts details like node IDs
 * and states dynamically, and logs them colorized to console and plaintext to 'smartfab.log'.
 */
public class LogUtils {

    // ANSI Escape Sequences for terminal styling
    public static final String ANSI_RESET = "\u001B[0m";
    public static final String ANSI_BOLD = "\u001B[1m";
    public static final String ANSI_RED = "\u001B[31m";
    public static final String ANSI_GREEN = "\u001B[32m";
    public static final String ANSI_YELLOW = "\u001B[33m";
    public static final String ANSI_BLUE = "\u001B[34m";
    public static final String ANSI_MAGENTA = "\u001B[35m";
    public static final String ANSI_CYAN = "\u001B[36m";
    public static final String ANSI_WHITE = "\u001B[37m";

    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");
    private static final String LOG_FILE_PATH = "smartfab.log";

    // References to the original standard output streams to prevent infinite recursion
    private static final java.io.PrintStream originalOut = System.out;
    private static final java.io.PrintStream originalErr = System.err;

    /**
     * Redirects System.out and System.err to our custom interceptor.
     */
    public static void redirectSystemOutAndErr(String defaultRole, String defaultRoleColor) {
        System.setOut(new java.io.PrintStream(originalOut) {
            @Override
            public void println(String x) {
                logIntercepted(defaultRole, defaultRoleColor, x, false);
            }
            @Override
            public void print(String x) {
                // If it is just a console prompt (no newline), bypass formatting and write directly
                originalOut.print(x);
            }
            @Override
            public void println(Object x) {
                println(String.valueOf(x));
            }
        });

        System.setErr(new java.io.PrintStream(originalErr) {
            @Override
            public void println(String x) {
                logIntercepted(defaultRole, defaultRoleColor, x, true);
            }
            @Override
            public void print(String x) {
                originalErr.print(x);
            }
            @Override
            public void println(Object x) {
                println(String.valueOf(x));
            }
        });
    }

    /**
     * Logs a message with timestamp and role to both the console (with ANSI color)
     * and the shared log file (plain text).
     */
    public static void log(String role, String state, String message, String roleColor, String stateColor) {
        String timestamp = LocalTime.now().format(formatter);
        
        // 1. Format Role
        String roleToken = "[" + role + "]";
        String paddedRoleToken = padRight(roleToken, 10);
        
        // 2. Format State
        String stateToken = (state != null && !state.isEmpty()) ? "[" + state + "]" : "";
        String paddedStateToken = padRight(stateToken, 26);

        // 3. Build Console Line with Colors
        StringBuilder consoleSb = new StringBuilder();
        consoleSb.append("[").append(timestamp).append("] ");
        
        if (roleColor != null) {
            consoleSb.append(roleColor).append(ANSI_BOLD).append(roleToken).append(ANSI_RESET);
            consoleSb.append(" ".repeat(Math.max(0, 10 - roleToken.length())));
        } else {
            consoleSb.append(paddedRoleToken);
        }
        consoleSb.append(" ");
        
        if (stateColor != null && !stateToken.isEmpty()) {
            consoleSb.append(stateColor).append(stateToken).append(ANSI_RESET);
            consoleSb.append(" ".repeat(Math.max(0, 26 - stateToken.length())));
        } else {
            consoleSb.append(paddedStateToken);
        }
        consoleSb.append(" ");
        
        consoleSb.append(message);
        originalOut.println(consoleSb.toString());

        // 4. Write the colorized console message to the shared log file so that 'tail -f' shows colors too
        writeToLogFile(consoleSb.toString());
    }

    private static String padRight(String text, int length) {
        if (text == null) {
            text = "";
        }
        if (text.length() >= length) {
            return text;
        }
        return text + " ".repeat(length - text.length());
    }

    /**
     * Intercepts standard console outputs, parses details (e.g. node ID, states)
     * dynamically, and outputs them in colorized format.
     */
    private static void logIntercepted(String defaultRole, String defaultRoleColor, String message, boolean isError) {
        if (message == null) {
            message = "null";
        }

        // 1. If it's a standard Spring Boot Logback framework log (starts with timestamp like "2026-"),
        // print it directly to the console and write it to the shared log file as-is.
        if (message.matches("^\\d{4}-\\d{2}-\\d{2}.*")) {
            originalOut.println(message);
            writeToLogFile(message);
            return;
        }

        // 2. Bypass formatting for UI headers or raw menu choices
        if (message.contains("Choice:") || message.contains("Select an operation:") || message.contains("==================") || message.contains("Choice:")) {
            originalOut.println(message);
            return;
        }

        String role = defaultRole;
        String roleColor = defaultRoleColor;
        String state = null;
        String stateColor = null;
        String cleanMessage = message;

        // Try to parse [PEER X] prefix
        if (cleanMessage.contains("[PEER ")) {
            int startIdx = cleanMessage.indexOf("[PEER ");
            int endIdx = cleanMessage.indexOf("]", startIdx);
            if (endIdx != -1) {
                String idStr = cleanMessage.substring(startIdx + 6, endIdx).trim();
                role = "PEER-" + idStr;
                switch (idStr) {
                    case "1": roleColor = ANSI_GREEN; break;
                    case "2": roleColor = ANSI_YELLOW; break;
                    case "3": roleColor = ANSI_MAGENTA; break;
                    default: roleColor = ANSI_BLUE; break;
                }
                cleanMessage = cleanMessage.substring(0, startIdx) + cleanMessage.substring(endIdx + 1);
            }
        }

        // Try to parse [ADMIN_SERVER] prefix
        if (cleanMessage.contains("[ADMIN_SERVER]")) {
            role = "SERVER";
            roleColor = ANSI_CYAN;
            cleanMessage = cleanMessage.replace("[ADMIN_SERVER]", "");
        }

        // Try to parse [ADMIN_CLIENT] prefix
        if (cleanMessage.contains("[ADMIN_CLIENT]")) {
            role = "CLIENT";
            roleColor = ANSI_BLUE;
            cleanMessage = cleanMessage.replace("[ADMIN_CLIENT]", "");
        }

        // Try to parse states dynamically
        for (String st : new String[]{"FULLY_OPERATIONAL", "WAITING_FOR_CALIBRATION", "UNDER_CALIBRATION"}) {
            String stateToken = "[" + st + "]";
            if (cleanMessage.contains(stateToken)) {
                state = st;
                switch (st) {
                    case "FULLY_OPERATIONAL": stateColor = ANSI_GREEN; break;
                    case "WAITING_FOR_CALIBRATION": stateColor = ANSI_YELLOW; break;
                    case "UNDER_CALIBRATION": stateColor = ANSI_RED; break;
                }
                cleanMessage = cleanMessage.replace(stateToken, "");
            }
        }

        // Add ERROR tag if printed to System.err
        if (isError && state == null) {
            state = "ERROR";
            stateColor = ANSI_RED;
        }

        // Clean up redundant spaces and trim
        cleanMessage = cleanMessage.replaceAll("\\s+", " ").trim();
        if (cleanMessage.isEmpty()) {
            return; // skip logging empty lines
        }

        log(role, state, cleanMessage, roleColor, stateColor);
    }

    /**
     * Appends a log line to 'smartfab.log' in a process-safe manner.
     */
    private static synchronized void writeToLogFile(String line) {
        try (FileWriter fw = new FileWriter(LOG_FILE_PATH, true);
             PrintWriter pw = new PrintWriter(fw)) {
            pw.println(line);
        } catch (Exception e) {
            // Silently ignore writing failures
        }
    }
}
