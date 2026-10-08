package com.cache.client.cli;

import com.cache.client.CacheClient;
import com.cache.client.ClusterCacheClient;
import com.cache.common.cluster.CacheNode;

import java.io.BufferedReader;
import java.io.Console;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Interactive shell for a JCache server or cluster.
 *
 * <pre>
 * java -jar cache-client.jar localhost 6379
 * java -jar cache-client.jar --cluster node-1:localhost:6379 node-2:localhost:6380
 * </pre>
 *
 * Arguments containing spaces can be quoted: {@code put greeting "hello world"}.
 */
public class CacheCLI {

    private static final String RESET = "\u001B[0m";
    private static final String GREEN = "\u001B[32m";
    private static final String RED = "\u001B[31m";
    private static final String YELLOW = "\u001B[33m";
    private static final String CYAN = "\u001B[36m";
    private static final String BOLD = "\u001B[1m";
    private static final String DIM = "\u001B[2m";

    private static final int MAX_HISTORY = 50;

    @FunctionalInterface
    private interface Handler {
        /** @return {@code false} to leave the shell */
        boolean handle(String[] args) throws Exception;
    }

    private final BufferedReader in;
    private final PrintStream out;
    private final PrintStream err;
    private final boolean color;
    private final Map<String, Handler> commands = new LinkedHashMap<>();
    private final Deque<String> history = new ArrayDeque<>();

    private CacheClient single;
    private ClusterCacheClient cluster;
    private String prompt;

    CacheCLI(InputStream in, PrintStream out, PrintStream err, boolean color) {
        this.in = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));
        this.out = out;
        this.err = err;
        this.color = color;
    }

    /**
     * Entry point.
     *
     * @param args {@code <host> <port>} or {@code --cluster <id:host:port>...},
     *             optionally with {@code --no-color}
     */
    public static void main(String[] args) {
        List<String> rest = new ArrayList<>(Arrays.asList(args));
        boolean noColor = rest.remove("--no-color");
        if (rest.contains("--version")) {
            System.out.println("JCache CLI " + version());
            return;
        }
        if (rest.isEmpty() || rest.contains("--help") || rest.contains("-h")) {
            System.out.println(usage());
            return;
        }

        CacheCLI cli = new CacheCLI(System.in, System.out, System.err, !noColor && useColor());
        try {
            if (rest.get(0).equals("--cluster")) {
                cli.connectCluster(rest.subList(1, rest.size()));
            } else if (rest.size() == 2) {
                cli.connectSingle(rest.get(0), Integer.parseInt(rest.get(1)));
            } else {
                throw new IllegalArgumentException("expected <host> <port>");
            }
        } catch (IOException | IllegalArgumentException | IllegalStateException e) {
            System.err.println("Error: " + e.getMessage());
            System.err.println(usage());
            System.exit(1);
            return;
        }
        try {
            cli.run();
        } finally {
            cli.disconnect();
        }
    }

    void connectSingle(String host, int port) throws IOException {
        single = new CacheClient(host, port);
        if (!single.ping()) {
            throw new IOException("Server at " + host + ":" + port + " did not answer PING");
        }
        prompt = paint(BOLD, "jcache") + paint(DIM, "[" + host + ":" + port + "]") + "> ";
        registerCommands(false);
        println(paint(GREEN, "Connected to " + host + ":" + port) + paint(DIM, ". Type 'help' for commands."));
    }

    void connectCluster(List<String> specs) throws IOException {
        if (specs.isEmpty()) {
            throw new IllegalArgumentException("--cluster needs at least one id:host:port");
        }
        ClusterCacheClient.Builder builder = ClusterCacheClient.builder().poolSizePerNode(2);
        for (String spec : specs) {
            String[] parts = spec.split(":");
            if (parts.length != 3) {
                throw new IllegalArgumentException("bad node spec '" + spec + "', expected id:host:port");
            }
            builder.addServer(parts[0], parts[1], Integer.parseInt(parts[2]));
        }
        cluster = builder.build();
        updateClusterPrompt();
        registerCommands(true);
        println(paint(GREEN, "Connected to " + specs.size() + " node(s)") + paint(DIM, ". Type 'help' for commands."));
    }

    void run() {
        while (true) {
            out.print(prompt);
            out.flush();
            String line;
            try {
                line = in.readLine();
            } catch (IOException e) {
                error("read failed: " + e.getMessage());
                return;
            }
            if (line == null) {
                println("");
                return;
            }
            line = line.trim();
            if (line.isEmpty()) {
                continue;
            }
            remember(line);

            String[] tokens = tokenize(line);
            Handler handler = commands.get(tokens[0].toLowerCase(Locale.ROOT));
            if (handler == null) {
                error("unknown command '" + tokens[0] + "'. Type 'help' for the list.");
                continue;
            }
            try {
                if (!handler.handle(Arrays.copyOfRange(tokens, 1, tokens.length))) {
                    return;
                }
            } catch (Exception e) {
                error(e.getMessage());
            }
        }
    }

    private void registerCommands(boolean clusterMode) {
        commands.put("get", this::get);
        commands.put("put", this::put);
        commands.put("set", this::put);
        commands.put("delete", this::delete);
        commands.put("del", this::delete);
        if (!clusterMode) {
            commands.put("expire", this::expire);
            commands.put("ttl", this::ttl);
            commands.put("persist", this::persist);
        }
        commands.put("ping", this::ping);
        commands.put("stats", this::stats);
        commands.put("flush", this::flush);
        if (clusterMode) {
            commands.put("nodes", this::nodes);
            commands.put("route", this::route);
            commands.put("distribution", this::distribution);
            commands.put("addnode", this::addNode);
            commands.put("removenode", this::removeNode);
        }
        commands.put("history", args -> {
            int i = 1;
            for (String h : history) {
                println(String.format("%4d  %s", i++, h));
            }
            return true;
        });
        commands.put("help", args -> {
            println(help(clusterMode));
            return true;
        });
        commands.put("exit", args -> false);
        commands.put("quit", args -> false);
    }

    private boolean get(String[] args) throws IOException {
        requireArgs(args, 1, "get <key>");
        String value = single != null ? single.get(args[0]) : cluster.get(args[0]);
        println(value == null ? paint(DIM, "(nil)") : "\"" + value + "\"");
        return true;
    }

    private boolean put(String[] args) throws IOException {
        requireArgs(args, 2, "put <key> <value> [ttlSeconds]");
        long ttl = 0;
        int valueEnd = args.length;
        if (args.length > 2 && args[args.length - 1].matches("\\d+")) {
            ttl = Long.parseLong(args[args.length - 1]);
            valueEnd--;
        }
        String value = String.join(" ", Arrays.copyOfRange(args, 1, valueEnd));
        if (single != null) {
            single.put(args[0], value, ttl);
        } else {
            cluster.put(args[0], value, ttl);
        }
        println(paint(GREEN, "OK") + (ttl > 0 ? paint(DIM, " (expires in " + formatSeconds(ttl) + ")") : ""));
        return true;
    }

    private boolean delete(String[] args) throws IOException {
        requireArgs(args, 1, "delete <key>");
        if (single != null) {
            single.delete(args[0]);
        } else {
            cluster.delete(args[0]);
        }
        println(paint(GREEN, "OK"));
        return true;
    }

    private boolean expire(String[] args) throws IOException {
        requireArgs(args, 2, "expire <key> <seconds>");
        boolean found = single.expire(args[0], Long.parseLong(args[1]));
        println(found ? paint(GREEN, "OK") : paint(DIM, "(key not found)"));
        return true;
    }

    private boolean ttl(String[] args) throws IOException {
        requireArgs(args, 1, "ttl <key>");
        long ttl = single.ttl(args[0]);
        println(ttl == -2 ? paint(DIM, "(key not found)") : ttl == -1 ? "no expiry" : formatSeconds(ttl));
        return true;
    }

    private boolean persist(String[] args) throws IOException {
        requireArgs(args, 1, "persist <key>");
        println(single.persist(args[0]) ? paint(GREEN, "OK") : paint(DIM, "(key not found)"));
        return true;
    }

    private boolean ping(String[] args) {
        if (single != null) {
            long start = System.nanoTime();
            boolean ok = single.ping();
            long micros = (System.nanoTime() - start) / 1000;
            println(ok ? paint(GREEN, "PONG") + paint(DIM, String.format(" (%.2f ms)", micros / 1000.0))
                    : paint(RED, "no reply"));
        } else {
            cluster.ping().forEach((id, ok) ->
                    println(String.format("%-12s %s", id, ok ? paint(GREEN, "PONG") : paint(RED, "no reply"))));
        }
        return true;
    }

    private boolean stats(String[] args) throws IOException {
        Map<String, String> stats = single != null ? single.stats() : cluster.clusterStats();
        stats.forEach((k, v) -> println(String.format("  %-22s %s", paint(DIM, k), v)));
        return true;
    }

    private boolean flush(String[] args) throws IOException {
        out.print(paint(YELLOW, "This deletes every key. Type 'yes' to continue: "));
        out.flush();
        String answer = in.readLine();
        if (!"yes".equalsIgnoreCase(answer == null ? "" : answer.trim())) {
            println(paint(DIM, "Cancelled."));
            return true;
        }
        if (single != null) {
            single.flush();
            println(paint(GREEN, "OK"));
        } else {
            Map<String, String> failures = cluster.flushAll();
            println(failures.isEmpty() ? paint(GREEN, "OK") : paint(RED, "Failed on: " + failures));
        }
        return true;
    }

    private boolean nodes(String[] args) {
        for (CacheNode node : cluster.getNodes()) {
            println(String.format("  %-12s %-22s %s", paint(CYAN, node.getId()), node.getAddress(), node.getStatus()));
        }
        return true;
    }

    private boolean route(String[] args) {
        requireArgs(args, 1, "route <key>");
        CacheNode node = cluster.getRoutingTarget(args[0]);
        println(args[0] + " -> " + paint(CYAN, node.getId()) + paint(DIM, " (" + node.getAddress() + ")"));
        return true;
    }

    private boolean distribution(String[] args) {
        cluster.getKeyDistribution().forEach((id, pct) -> {
            int bar = (int) Math.round(pct / 100 * 40);
            println(String.format("  %-12s %6.2f%%  %s", id, pct, paint(GREEN, "#".repeat(bar))));
        });
        return true;
    }

    private boolean addNode(String[] args) throws IOException {
        requireArgs(args, 3, "addnode <id> <host> <port>");
        cluster.addServer(args[0], args[1], Integer.parseInt(args[2]));
        updateClusterPrompt();
        println(paint(GREEN, "Added " + args[0]));
        return true;
    }

    private boolean removeNode(String[] args) {
        requireArgs(args, 1, "removenode <id>");
        cluster.removeServer(args[0]);
        updateClusterPrompt();
        println(paint(GREEN, "Removed " + args[0]));
        return true;
    }

    private void updateClusterPrompt() {
        prompt = paint(BOLD, "jcache") + paint(DIM, "[cluster:" + cluster.getNodeCount() + "]") + "> ";
    }

    private void disconnect() {
        if (single != null) {
            single.close();
        }
        if (cluster != null) {
            cluster.close();
        }
    }

    private void remember(String line) {
        if (history.size() == MAX_HISTORY) {
            history.removeFirst();
        }
        history.addLast(line);
    }

    private static void requireArgs(String[] args, int min, String usage) {
        if (args.length < min) {
            throw new IllegalArgumentException("usage: " + usage);
        }
    }

    /** Splits on spaces, treating double-quoted text as one token. */
    static String[] tokenize(String line) {
        List<String> tokens = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean quoted = false;
        for (char c : line.toCharArray()) {
            if (c == '"') {
                quoted = !quoted;
            } else if (Character.isWhitespace(c) && !quoted) {
                if (current.length() > 0) {
                    tokens.add(current.toString());
                    current.setLength(0);
                }
            } else {
                current.append(c);
            }
        }
        if (current.length() > 0) {
            tokens.add(current.toString());
        }
        return tokens.toArray(new String[0]);
    }

    static String formatSeconds(long seconds) {
        long d = seconds / 86_400;
        long h = seconds % 86_400 / 3600;
        long m = seconds % 3600 / 60;
        long s = seconds % 60;
        StringBuilder sb = new StringBuilder();
        if (d > 0) {
            sb.append(d).append("d ");
        }
        if (h > 0) {
            sb.append(h).append("h ");
        }
        if (m > 0) {
            sb.append(m).append("m ");
        }
        if (s > 0 || sb.length() == 0) {
            sb.append(s).append('s');
        }
        return sb.toString().trim();
    }

    private String paint(String ansi, String text) {
        return color ? ansi + text + RESET : text;
    }

    private void println(String s) {
        out.println(s);
    }

    private void error(String message) {
        err.println(paint(RED, "(error) ") + message);
    }

    private static String help(boolean clusterMode) {
        StringBuilder sb = new StringBuilder()
                .append("  get <key>                       read a value\n")
                .append("  put <key> <value> [ttl]         store a value; ttl in seconds\n")
                .append("  delete <key>                    remove a key\n");
        if (!clusterMode) {
            sb.append("  expire <key> <seconds>          set a key's TTL (0 deletes it)\n")
              .append("  ttl <key>                       show a key's remaining TTL\n")
              .append("  persist <key>                   remove a key's TTL\n");
        }
        sb.append("  ping | stats | flush\n");
        if (clusterMode) {
            sb.append("  nodes | distribution | route <key>\n")
              .append("  addnode <id> <host> <port> | removenode <id>\n");
        }
        return sb.append("  history | help | exit").toString();
    }

    /** Colour only on an interactive terminal, and never when NO_COLOR is set (no-color.org). */
    private static boolean useColor() {
        Console console = System.console();
        if (console == null || System.getenv("NO_COLOR") != null) {
            return false;
        }
        try {
            // Since JDK 22 System.console() is non-null even when output is redirected.
            return (Boolean) Console.class.getMethod("isTerminal").invoke(console);
        } catch (ReflectiveOperationException e) {
            return true;
        }
    }

    private static String usage() {
        return String.join(System.lineSeparator(),
                "Usage:",
                "  java -jar cache-client.jar <host> <port> [--no-color]",
                "  java -jar cache-client.jar --cluster <id:host:port>... [--no-color]",
                "  java -jar cache-client.jar --version");
    }

    private static String version() {
        try (InputStream is = CacheCLI.class.getResourceAsStream("/version.txt")) {
            return is == null ? "unknown" : new String(is.readAllBytes(), StandardCharsets.UTF_8).trim();
        } catch (IOException e) {
            return "unknown";
        }
    }
}
