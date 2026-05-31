package com.cache.client.cli;

import com.cache.client.CacheClient;
import com.cache.client.ClusterCacheClient;
import com.cache.common.cluster.CacheNode;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.util.*;

/**
 * CacheCLI — interactive command-line shell for JCache.
 *
 * ═══════════════════════════════════════════════════════════════════════
 * TWO MODES
 * ═══════════════════════════════════════════════════════════════════════
 *
 * SINGLE-NODE MODE (default):
 *   Connects one CacheClient to one server.
 *   Every command goes to that server directly.
 *
 *   java -jar cache-cli.jar localhost 6379
 *
 * CLUSTER MODE (--cluster flag):
 *   Connects ClusterCacheClient to multiple servers.
 *   get/put/delete route via consistent hashing.
 *   nodes/distribution/stats fan out across all nodes.
 *
 *   java -jar cache-cli.jar --cluster node1:6379 node2:6379 node3:6379
 *
 * ═══════════════════════════════════════════════════════════════════════
 * COMMANDS
 * ═══════════════════════════════════════════════════════════════════════
 *
 * Both modes:
 *   get <key>                   retrieve value (null → "(nil)")
 *   put <key> <value>           store with no TTL
 *   put <key> <value> <ttl>     store with TTL in seconds
 *   delete <key>                remove key (idempotent)
 *   ping                        server health check
 *   flush                       clear ALL keys (single) / ALL nodes (cluster)
 *   stats                       show server metrics
 *   help                        list commands
 *   exit / quit                 close connection and exit
 *
 * Cluster-only:
 *   nodes                       list all cluster nodes with addresses
 *   distribution                show key distribution % per node
 *   route <key>                 show which node a key would route to
 *   addnode <id> <host> <port>  add a node to the cluster at runtime
 *   removenode <id>             remove a node from the cluster
 *
 * ═══════════════════════════════════════════════════════════════════════
 * DESIGN DECISIONS
 * ═══════════════════════════════════════════════════════════════════════
 *
 * BufferedReader over System.in — not JLine or Readline.
 * JLine adds a dependency and requires native libraries for arrow-key
 * history support. BufferedReader works everywhere (CI, Docker, pipes)
 * with zero extra dependencies. We print a PS1-style prompt before each
 * read, which is the standard pattern for pure-Java REPLs.
 *
 * Dispatcher map over switch statement.
 * We store command handlers in a Map<String, CommandHandler> rather than
 * a giant switch. This makes adding new commands trivial (one line in the
 * map), keeps each handler small and testable, and avoids the fall-through
 * risk of switch. The map lookup is O(1) vs O(N) linear scan.
 *
 * Unified output via PrintStream (not System.out directly).
 * The `out` field is injectable — tests can pass a ByteArrayOutputStream
 * to capture output without touching System.out. Same for `err`.
 *
 * Error responses shown as-is from server.
 * When the server returns "-ERR something", we strip the "-ERR " prefix
 * and print it in red (if color is enabled). We don't swallow server
 * errors — showing them raw is more useful for debugging.
 *
 * ═══════════════════════════════════════════════════════════════════════
 * USAGE EXAMPLES
 * ═══════════════════════════════════════════════════════════════════════
 *
 * Single node:
 *   $ java -jar cache-cli.jar localhost 6379
 *   jcache[localhost:6379]> put name Alice
 *   OK
 *   jcache[localhost:6379]> get name
 *   "Alice"
 *   jcache[localhost:6379]> put session token123 3600
 *   OK
 *   jcache[localhost:6379]> stats
 *   hits:1 misses:0 gets:1 puts:2 ...
 *   jcache[localhost:6379]> exit
 *   Bye!
 *
 * Cluster:
 *   $ java -jar cache-cli.jar --cluster node-1:6379 node-2:6380 node-3:6381
 *   jcache[cluster:3 nodes]> nodes
 *   node-1  localhost:6379  ACTIVE
 *   node-2  localhost:6380  ACTIVE
 *   node-3  localhost:6381  ACTIVE
 *   jcache[cluster:3 nodes]> route user:42
 *   key "user:42" → node-2 (localhost:6380)
 *   jcache[cluster:3 nodes]> distribution
 *   node-1   33.78%  ████████████████
 *   node-2   32.44%  ████████████████
 *   node-3   33.78%  ████████████████
 */
public class CacheCLI {

    // -------------------------------------------------------------------------
    // ANSI color codes — used when terminal supports color
    // -------------------------------------------------------------------------

    private static final String ANSI_RESET  = "\u001B[0m";
    private static final String ANSI_GREEN  = "\u001B[32m";
    private static final String ANSI_RED    = "\u001B[31m";
    private static final String ANSI_YELLOW = "\u001B[33m";
    private static final String ANSI_CYAN   = "\u001B[36m";
    private static final String ANSI_BOLD   = "\u001B[1m";
    private static final String ANSI_DIM    = "\u001B[2m";

    // -------------------------------------------------------------------------
    // State
    // -------------------------------------------------------------------------

    /** Output stream — injectable for tests. */
    private final PrintStream out;

    /** Error stream — injectable for tests. */
    private final PrintStream err;

    /**
     * true if ANSI color codes should be emitted.
     * Disabled automatically when output is not a TTY (piped, redirected).
     * Can be forced off with --no-color flag.
     */
    private final boolean colorEnabled;

    /**
     * true if running in cluster mode.
     * Determines which client is used and which commands are available.
     */
    private final boolean clusterMode;

    /**
     * Single-node client. Non-null only in single-node mode.
     * Null in cluster mode.
     */
    private CacheClient singleClient;

    /**
     * Cluster client. Non-null only in cluster mode.
     * Null in single-node mode.
     */
    private ClusterCacheClient clusterClient;

    /**
     * The prompt string shown before each input line.
     * Set once after connection is established.
     * Examples:
     *   "jcache[localhost:6379]> "
     *   "jcache[cluster:3 nodes]> "
     */
    private String prompt;

    /**
     * Registered command handlers, keyed by command name (lowercase).
     * Populated in buildCommandMap().
     * Lookup is O(1) — faster than a switch and easier to extend.
     */
    private final Map<String, CommandHandler> commands;

    /**
     * Command history — the last 50 commands entered.
     * Not used for arrow-key navigation (requires JLine), but printed
     * by the "history" command for reference.
     */
    private final Deque<String> history;

    private static final int MAX_HISTORY = 50;

    // -------------------------------------------------------------------------
    // Functional interface for command handlers
    // -------------------------------------------------------------------------

    /**
     * A command handler processes one parsed command line.
     *
     * //@param args All tokens after the command verb.
     *             e.g. for "put foo bar 60" → args = ["foo", "bar", "60"]
     * @return true to continue the REPL loop, false to exit.
     */
    @FunctionalInterface
    private interface CommandHandler {
        boolean handle(String[] args);
    }

    // -------------------------------------------------------------------------
    // Constructor
    // -------------------------------------------------------------------------

    /**
     * Creates a CacheCLI with the given output streams and color preference.
     *
     * @param out          Where to print output (normally System.out).
     * @param err          Where to print errors (normally System.err).
     * @param colorEnabled Whether to use ANSI color codes in output.
     * @param clusterMode  true if this CLI will connect to a cluster.
     */
    public CacheCLI(PrintStream out, PrintStream err,
                    boolean colorEnabled, boolean clusterMode) {
        this.out          = out;
        this.err          = err;
        this.colorEnabled = colorEnabled;
        this.clusterMode  = clusterMode;
        this.commands     = new LinkedHashMap<>(); // LinkedHashMap preserves insertion order for help
        this.history      = new ArrayDeque<>();
    }

    // -------------------------------------------------------------------------
    // Entry point — main()
    // -------------------------------------------------------------------------

    /**
     * Main entry point.
     *
     * Argument formats:
     *
     *   Single-node:
     *     cache-cli <host> <port> [--no-color]
     *     cache-cli localhost 6379
     *     cache-cli 10.0.1.5 6379 --no-color
     *
     *   Cluster:
     *     cache-cli --cluster <id:host:port> [<id:host:port> ...] [--no-color]
     *     cache-cli --cluster node-1:localhost:6379 node-2:localhost:6380
     *
     *     Each cluster node spec is: nodeId:host:port
     *     nodeId cannot contain colons. host can be an IP or hostname.
     *     Port is the last colon-delimited segment.
     *
     * @param args Command-line arguments.
     */
    public static void main(String[] args) {
        // Detect color support: enable if stdout is a real terminal (TTY).
        // System.console() returns null when output is piped or redirected.
        boolean colorSupported = System.console() != null;

        // Parse --no-color flag anywhere in args.
        boolean noColor = Arrays.asList(args).contains("--no-color");
        boolean colorEnabled = colorSupported && !noColor;

        // Strip --no-color from args before further parsing.
        args = Arrays.stream(args)
                .filter(a -> !a.equals("--no-color"))
                .toArray(String[]::new);

        if (args.length == 0) {
            printUsageAndExit();
        }

        boolean clusterMode = args[0].equals("--cluster");

        CacheCLI cli = new CacheCLI(System.out, System.err, colorEnabled, clusterMode);

        try {
            if (clusterMode) {
                cli.connectCluster(args);
            } else {
                cli.connectSingle(args);
            }
            cli.run();
        } catch (IOException e) {
            System.err.println("Connection failed: " + e.getMessage());
            System.exit(1);
        } finally {
            cli.disconnect();
        }
    }

    // -------------------------------------------------------------------------
    // Connection setup
    // -------------------------------------------------------------------------

    /**
     * Connects to a single cache server.
     *
     * @param args ["host", "port"] from command line.
     * @throws IOException if connection fails.
     */
    private void connectSingle(String[] args) throws IOException {
        if (args.length < 2) {
            printLine(color(ANSI_RED, "Error: single-node mode requires <host> <port>"));
            printLine("Usage: cache-cli <host> <port>");
            System.exit(1);
        }

        String host;
        int    port;
        try {
            host = args[0];
            port = Integer.parseInt(args[1]);
        } catch (NumberFormatException e) {
            printLine(color(ANSI_RED, "Error: port must be an integer, got: " + args[1]));
            System.exit(1);
            return;
        }

        printLine(color(ANSI_DIM, "Connecting to " + host + ":" + port + "..."));

        singleClient = new CacheClient(host, port);

        // Verify connection with PING.
        if (!singleClient.ping()) {
            throw new IOException("Server at " + host + ":" + port
                    + " did not respond to PING. Is it running?");
        }

        prompt = color(ANSI_BOLD, "jcache") + color(ANSI_DIM, "[" + host + ":" + port + "]")
                + color(ANSI_GREEN, "> ") + ANSI_RESET;

        printLine(color(ANSI_GREEN, "✓ Connected to " + host + ":" + port));
        printLine(color(ANSI_DIM, "Type 'help' for available commands, 'exit' to quit."));

        buildCommandMap();
    }

    /**
     * Connects to a cluster of cache servers.
     *
     * @param args ["--cluster", "id:host:port", "id:host:port", ...] from command line.
     * @throws IOException if any node connection fails.
     */
    private void connectCluster(String[] args) throws IOException {
        // args[0] is "--cluster", node specs start at args[1].
        if (args.length < 2) {
            printLine(color(ANSI_RED,
                    "Error: --cluster requires at least one node spec (id:host:port)"));
            System.exit(1);
        }

        ClusterCacheClient.Builder builder = ClusterCacheClient.builder()
                .poolSizePerNode(3);

        int nodeCount = 0;
        for (int i = 1; i < args.length; i++) {
            String spec = args[i];

            // Parse "nodeId:host:port" — split from the RIGHT so host can be an IP.
            // "node-1:192.168.1.10:6379" → ["node-1", "192.168.1.10", "6379"]
            // We find the LAST colon for port, second-to-last for host boundary.
            int lastColon       = spec.lastIndexOf(':');
            int secondLastColon = spec.lastIndexOf(':', lastColon - 1);

            if (lastColon < 0 || secondLastColon < 0) {
                printLine(color(ANSI_RED,
                        "Invalid node spec '" + spec + "'. Expected format: nodeId:host:port"));
                System.exit(1);
            }

            String nodeId = spec.substring(0, secondLastColon);
            String host   = spec.substring(secondLastColon + 1, lastColon);
            int    port;
            try {
                port = Integer.parseInt(spec.substring(lastColon + 1));
            } catch (NumberFormatException e) {
                printLine(color(ANSI_RED,
                        "Invalid port in node spec '" + spec + "'"));
                System.exit(1);
                return;
            }

            printLine(color(ANSI_DIM, "Connecting to [" + nodeId + "] at " + host + ":" + port + "..."));
            builder.addServer(nodeId, host, port);
            nodeCount++;
        }

        clusterClient = builder.build();

        prompt = color(ANSI_BOLD, "jcache") + color(ANSI_DIM, "[cluster:" + nodeCount + " nodes]")
                + color(ANSI_GREEN, "> ") + ANSI_RESET;

        printLine(color(ANSI_GREEN, "✓ Connected to " + nodeCount + "-node cluster"));
        printLine(color(ANSI_DIM, "Type 'help' for available commands, 'nodes' to list cluster nodes."));

        buildCommandMap();
    }

    // -------------------------------------------------------------------------
    // REPL — Read-Eval-Print Loop
    // -------------------------------------------------------------------------

    /**
     * The main REPL loop. Reads one line at a time, dispatches to handlers.
     * Runs until a handler returns false (exit/quit command) or EOF (Ctrl+D).
     */
    private void run() {
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));

        while (true) {
            // Print the prompt without a newline — user types on the same line.
            out.print(prompt);
            out.flush();

            String line;
            try {
                line = reader.readLine();
            } catch (IOException e) {
                printErr("Read error: " + e.getMessage());
                break;
            }

            // null means EOF (Ctrl+D on Unix, Ctrl+Z on Windows).
            if (line == null) {
                printLine(""); // newline after ^D
                printLine(color(ANSI_DIM, "Bye!"));
                break;
            }

            line = line.trim();

            // Empty line — show prompt again, no error.
            if (line.isEmpty()) {
                continue;
            }

            // Add to history (capped at MAX_HISTORY).
            addToHistory(line);

            // Tokenize: split on whitespace, preserving quoted strings.
            String[] tokens = tokenize(line);
            if (tokens.length == 0) continue;

            String verb    = tokens[0].toLowerCase();
            String[] args  = Arrays.copyOfRange(tokens, 1, tokens.length);

            // Look up handler.
            CommandHandler handler = commands.get(verb);
            if (handler == null) {
                printErr("Unknown command: " + color(ANSI_YELLOW, verb)
                        + ". Type 'help' for available commands.");
                continue;
            }

            // Execute handler. false return value means "exit the REPL".
            boolean continueLoop;
            try {
                continueLoop = handler.handle(args);
            } catch (Exception e) {
                // Catch unexpected errors so the REPL never crashes on bad input.
                printErr("Unexpected error: " + e.getMessage());
                continueLoop = true;
            }

            if (!continueLoop) {
                break;
            }
        }
    }

    // -------------------------------------------------------------------------
    // Command map construction
    // -------------------------------------------------------------------------

    /**
     * Registers all command handlers in the dispatch map.
     *
     * LinkedHashMap preserves insertion order, which is used by the "help"
     * command to print commands in a logical group order.
     *
     * Cluster-only commands are registered only when clusterMode is true.
     * Calling them in single-node mode would throw NullPointerException on
     * clusterClient — registering conditionally prevents that entirely.
     */
    private void buildCommandMap() {

        // ---- Core cache operations ----------------------------------------

        commands.put("get",    this::handleGet);
        commands.put("put",    this::handlePut);
        commands.put("set",    this::handlePut);    // alias: SET = PUT
        commands.put("delete", this::handleDelete);
        commands.put("del",    this::handleDelete); // alias: DEL = DELETE

        // ---- Server operations -------------------------------------------

        commands.put("ping",   this::handlePing);
        commands.put("flush",  this::handleFlush);
        commands.put("stats",  this::handleStats);

        // ---- Cluster-only commands ----------------------------------------

        if (clusterMode) {
            commands.put("nodes",      this::handleNodes);
            commands.put("distribution", this::handleDistribution);
            commands.put("route",      this::handleRoute);
            commands.put("addnode",    this::handleAddNode);
            commands.put("removenode", this::handleRemoveNode);
        }

        // ---- Utility -------------------------------------------------------

        commands.put("history", this::handleHistory);
        commands.put("clear",   this::handleClear);
        commands.put("help",    this::handleHelp);
        commands.put("exit",    this::handleExit);
        commands.put("quit",    this::handleExit); // alias: QUIT = EXIT
    }

    // =========================================================================
    // Command handlers — core operations
    // =========================================================================

    /**
     * GET <key>
     *
     * Retrieves a value from the cache.
     * Prints "(nil)" for cache misses — same as Redis CLI convention.
     * This makes it visually obvious that null was returned, not an empty string.
     *
     * Examples:
     *   > get name       → "Alice"
     *   > get missing    → (nil)
     */
    private boolean handleGet(String[] args) {
        if (args.length < 1) {
            printErr("Usage: get <key>");
            return true;
        }

        String key = args[0];
        try {
            String value = clusterMode
                    ? clusterClient.get(key)
                    : singleClient.get(key);

            if (value == null) {
                // Cache miss — print "(nil)" in dim style, same as Redis CLI.
                printLine(color(ANSI_DIM, "(nil)"));
            } else {
                // Cache hit — print value in quotes so whitespace is visible.
                printLine(color(ANSI_GREEN, "\"" + value + "\""));
            }

        } catch (Exception e) {
            printErr(e.getMessage());
        }
        return true;
    }

    /**
     * PUT <key> <value> [ttlSeconds]
     * SET <key> <value> [ttlSeconds]   ← alias
     *
     * Stores a key-value pair.
     * If ttlSeconds is provided and > 0, the key expires after that many seconds.
     * TTL=0 means no expiry (same as omitting TTL).
     *
     * Value can contain spaces — everything after the key (and before optional TTL)
     * is the value. The last token is treated as TTL only if it's a valid integer.
     *
     * Examples:
     *   > put name Alice
     *   OK
     *   > put session token123 3600
     *   OK  (expires in 1 hour)
     *   > put greeting hello world
     *   OK  (value is "hello world")
     */
    private boolean handlePut(String[] args) {
        if (args.length < 2) {
            printErr("Usage: put <key> <value> [ttlSeconds]");
            return true;
        }

        String key = args[0];

        // Check if the last token is a valid non-negative integer (TTL).
        // If so, value is everything between key and TTL.
        // If not, value is everything after key.
        String lastArg = args[args.length - 1];
        long   ttl     = 0;
        String value;

        if (args.length >= 3 && isNonNegativeLong(lastArg)) {
            // Last arg is TTL — value is middle args joined by space.
            ttl   = Long.parseLong(lastArg);
            value = String.join(" ", Arrays.copyOfRange(args, 1, args.length - 1));
        } else {
            // No TTL — value is all args after key.
            value = String.join(" ", Arrays.copyOfRange(args, 1, args.length));
        }

        if (value.isEmpty()) {
            printErr("Value cannot be empty.");
            return true;
        }

        try {
            if (clusterMode) {
                if (ttl > 0) {
                    clusterClient.put(key, value, ttl);
                } else {
                    clusterClient.put(key, value);
                }
            } else {
                if (ttl > 0) {
                    singleClient.put(key, value, ttl);
                } else {
                    singleClient.put(key, value);
                }
            }

            // Show OK with TTL info if a TTL was set.
            if (ttl > 0) {
                printLine(color(ANSI_GREEN, "OK")
                        + color(ANSI_DIM, " (expires in " + formatTTL(ttl) + ")"));
            } else {
                printLine(color(ANSI_GREEN, "OK"));
            }

        } catch (Exception e) {
            printErr(e.getMessage());
        }
        return true;
    }

    /**
     * DELETE <key>
     * DEL <key>    ← alias
     *
     * Removes a key from the cache. Idempotent — no error if key doesn't exist.
     *
     * Example:
     *   > delete name
     *   OK
     *   > del name
     *   OK  (already gone — still OK)
     */
    private boolean handleDelete(String[] args) {
        if (args.length < 1) {
            printErr("Usage: delete <key>");
            return true;
        }

        String key = args[0];
        try {
            if (clusterMode) {
                clusterClient.delete(key);
            } else {
                singleClient.delete(key);
            }
            printLine(color(ANSI_GREEN, "OK"));

        } catch (Exception e) {
            printErr(e.getMessage());
        }
        return true;
    }

    // =========================================================================
    // Command handlers — server operations
    // =========================================================================

    /**
     * PING
     *
     * Sends a health check to the server(s).
     * In cluster mode, pings the node that "ping-test" would route to.
     * This is a quick way to verify the server is alive.
     *
     * Example:
     *   > ping
     *   PONG  (127ms)
     */
    private boolean handlePing(String[] args) {
        try {
            long start = System.currentTimeMillis();

            boolean alive;
            if (clusterMode) {
                // In cluster mode, ping by doing a get on a sentinel key.
                // If routing works, the server responded.
                clusterClient.get("__ping__");
                alive = true;
            } else {
                alive = singleClient.ping();
            }

            long elapsed = System.currentTimeMillis() - start;

            if (alive) {
                printLine(color(ANSI_GREEN, "PONG")
                        + color(ANSI_DIM, " (" + elapsed + "ms)"));
            } else {
                printLine(color(ANSI_RED, "No response from server."));
            }

        } catch (Exception e) {
            printErr("Ping failed: " + e.getMessage());
        }
        return true;
    }

    /**
     * FLUSH
     *
     * Removes ALL keys from the cache.
     * In cluster mode, fans out to all nodes.
     * Prints a confirmation prompt before executing (destructive operation).
     *
     * Example:
     *   > flush
     *   WARNING: This will delete ALL keys. Type 'yes' to confirm: yes
     *   OK — all keys cleared.
     */
    private boolean handleFlush(String[] args) {
        // Confirm before destroying all data.
        out.print(color(ANSI_YELLOW,
                "WARNING: This will delete ALL keys. Type 'yes' to confirm: "));
        out.flush();

        String confirm;
        try {
            confirm = new BufferedReader(new InputStreamReader(System.in)).readLine();
        } catch (IOException e) {
            printErr("Could not read confirmation.");
            return true;
        }

        if (!"yes".equalsIgnoreCase(confirm == null ? "" : confirm.trim())) {
            printLine(color(ANSI_DIM, "Flush cancelled."));
            return true;
        }

        try {
            if (clusterMode) {
                Map<String, String> failures = clusterClient.flushAll();
                if (failures.isEmpty()) {
                    printLine(color(ANSI_GREEN, "OK")
                            + color(ANSI_DIM, " — all nodes cleared."));
                } else {
                    printLine(color(ANSI_YELLOW,
                            "Partial flush — some nodes failed:"));
                    for (Map.Entry<String, String> e : failures.entrySet()) {
                        printLine("  " + color(ANSI_RED, e.getKey())
                                + " → " + e.getValue());
                    }
                }
            } else {
                singleClient.flush();
                printLine(color(ANSI_GREEN, "OK")
                        + color(ANSI_DIM, " — all keys cleared."));
            }

        } catch (Exception e) {
            printErr("Flush failed: " + e.getMessage());
        }
        return true;
    }

    /**
     * STATS
     *
     * Displays server metrics in a formatted table.
     *
     * Single-node: shows metrics from the one connected server.
     * Cluster mode: shows aggregated cluster stats plus per-node breakdown.
     *
     * The stats line from the server is:
     *   hits:N misses:N gets:N puts:N deletes:N errors:N hitRate:X.XX%
     *   opsPerSec:X activeConn:N cacheSize:N totalConn:N uptime:Nms
     *   p50:Xms p99:Xms mean:Xms
     *
     * We parse this into a formatted table for readability.
     */
    private boolean handleStats(String[] args) {
        try {
            if (clusterMode) {
                printClusterStats();
            } else {
                printSingleNodeStats();
            }
        } catch (Exception e) {
            printErr("Failed to get stats: " + e.getMessage());
        }
        return true;
    }

    /**
     * Fetches and prints stats from the single connected node.
     * Parses the server's stats map into a readable table.
     */
    private void printSingleNodeStats() throws IOException {
        Map<String, String> stats = singleClient.stats();
        if (stats.isEmpty()) {
            printLine(color(ANSI_DIM, "(no stats available)"));
            return;
        }

        printLine("");
        printLine(color(ANSI_BOLD, "  ┌─ Server Statistics ─────────────────────────┐"));
        printStatRow("Hits",             stats.get("hits"));
        printStatRow("Misses",           stats.get("misses"));
        printStatRow("Hit Rate",         stats.get("hitRate"));
        printStatRow("Gets",             stats.get("gets"));
        printStatRow("Puts",             stats.get("puts"));
        printStatRow("Deletes",          stats.get("deletes"));
        printStatRow("Errors",           stats.get("errors"));
        printStatRow("Cache Size",       stats.get("size") != null
                ? stats.get("size") : stats.get("cacheSize"));
        printStatRow("Active Conn",      stats.get("activeConn"));
        printStatRow("Total Conn",       stats.get("totalConn"));
        printStatRow("Uptime",           formatUptime(stats.get("uptime")));
        printStatRow("Ops/sec",          stats.get("opsPerSec"));
        printStatRow("p50 Latency",      stats.get("p50"));
        printStatRow("p99 Latency",      stats.get("p99"));
        printStatRow("Mean Latency",     stats.get("mean"));
        printLine(color(ANSI_BOLD, "  └─────────────────────────────────────────────┘"));
        printLine("");
    }

    /**
     * Fetches and prints aggregated stats from all cluster nodes.
     * First shows cluster-wide totals, then per-node breakdown.
     */
    private void printClusterStats() {
        Map<String, String> stats = clusterClient.clusterStats();

        printLine("");
        printLine(color(ANSI_BOLD, "  ┌─ Cluster Statistics ────────────────────────┐"));
        printStatRow("Nodes",            stats.get("cluster.nodes"));
        printStatRow("Reachable",        stats.get("cluster.reachable"));
        printStatRow("Total Hits",       stats.get("cluster.totalHits"));
        printStatRow("Total Misses",     stats.get("cluster.totalMisses"));
        printStatRow("Cluster Hit Rate", stats.get("cluster.hitRate"));
        printLine(color(ANSI_BOLD, "  ├─ Per-Node Breakdown ───────────────────────┤"));

        // Group keys by node ID — find all distinct node prefixes.
        Set<String> nodeIds = new LinkedHashSet<>();
        for (String key : stats.keySet()) {
            if (!key.startsWith("cluster.")) {
                int dot = key.indexOf('.');
                if (dot > 0) nodeIds.add(key.substring(0, dot));
            }
        }

        for (String nodeId : nodeIds) {
            String status = stats.get(nodeId + ".status");
            if ("UNREACHABLE".equals(status)) {
                printLine("  │  " + color(ANSI_RED, nodeId) + ": UNREACHABLE");
            } else {
                String hits    = stats.get(nodeId + ".hits");
                String misses  = stats.get(nodeId + ".misses");
                String hitRate = stats.get(nodeId + ".hitRate");
                String size    = stats.get(nodeId + ".size") != null
                        ? stats.get(nodeId + ".size")
                        : stats.get(nodeId + ".cacheSize");
                printLine("  │  " + color(ANSI_CYAN, nodeId)
                        + "  hits=" + nvl(hits)
                        + "  misses=" + nvl(misses)
                        + "  hitRate=" + nvl(hitRate)
                        + "  size=" + nvl(size));
            }
        }

        printLine(color(ANSI_BOLD, "  └─────────────────────────────────────────────┘"));
        printLine("");
    }

    // =========================================================================
    // Command handlers — cluster-only
    // =========================================================================

    /**
     * NODES
     *
     * Lists all nodes currently in the cluster ring.
     * Shows: node ID, address, status.
     *
     * Example:
     *   > nodes
     *   ID         Address            Status
     *   ────────── ────────────────── ──────
     *   node-1     localhost:6379     ACTIVE
     *   node-2     localhost:6380     ACTIVE
     *   node-3     localhost:6381     ACTIVE
     *   3 nodes registered.
     */
    private boolean handleNodes(String[] args) {
        List<CacheNode> nodes = clusterClient.getNodes();

        if (nodes.isEmpty()) {
            printLine(color(ANSI_YELLOW, "No nodes registered in the cluster."));
            return true;
        }

        printLine("");
        // Header row.
        printLine(String.format("  %-15s %-22s %s",
                color(ANSI_BOLD, "ID"),
                color(ANSI_BOLD, "Address"),
                color(ANSI_BOLD, "Status")));
        printLine(color(ANSI_DIM,
                "  ─────────────── ────────────────────── ──────────"));

        for (CacheNode node : nodes) {
            String statusColor = node.isActive() ? ANSI_GREEN : ANSI_RED;
            printLine(String.format("  %-15s %-22s %s",
                    color(ANSI_CYAN, node.getId()),
                    node.getAddress(),
                    color(statusColor, node.getStatus().name())));
        }

        printLine("");
        printLine(color(ANSI_DIM,
                "  " + nodes.size() + " node(s) registered."));
        printLine("");
        return true;
    }

    /**
     * DISTRIBUTION
     *
     * Shows the approximate key distribution across all cluster nodes.
     * Each node should own ~1/N of the key space with consistent hashing.
     * Renders a simple ASCII bar chart for visual comparison.
     *
     * Example:
     *   > distribution
     *   node-1   33.78%  ████████████████░░░░░░░░░░░░░░░░
     *   node-2   32.44%  ███████████████░░░░░░░░░░░░░░░░░
     *   node-3   33.78%  ████████████████░░░░░░░░░░░░░░░░
     */
    private boolean handleDistribution(String[] args) {
        Map<String, Double> dist = clusterClient.getKeyDistribution();

        if (dist.isEmpty()) {
            printLine(color(ANSI_YELLOW, "No nodes registered."));
            return true;
        }

        printLine("");
        printLine(color(ANSI_BOLD, "  Key Distribution (consistent hash ring):"));
        printLine("");

        int barWidth = 32; // total bar width in characters

        for (Map.Entry<String, Double> entry : dist.entrySet()) {
            String nodeId  = entry.getKey();
            double percent = entry.getValue();

            int    filled  = (int) Math.round(percent / 100.0 * barWidth);
            int    empty   = barWidth - filled;

            String bar = color(ANSI_GREEN,  "█".repeat(Math.max(0, filled)))
                    + color(ANSI_DIM,    "░".repeat(Math.max(0, empty)));

            printLine(String.format("  %-12s  %5.2f%%  %s",
                    color(ANSI_CYAN, nodeId), percent, bar));
        }

        printLine("");
        return true;
    }

    /**
     * ROUTE <key>
     *
     * Shows which cluster node a given key would be routed to,
     * without actually performing any cache operation.
     *
     * Useful for debugging: "why is this key always going to node-2?"
     *
     * Example:
     *   > route user:42
     *   "user:42" → node-2 (localhost:6380)
     */
    private boolean handleRoute(String[] args) {
        if (args.length < 1) {
            printErr("Usage: route <key>");
            return true;
        }

        String key = args[0];
        try {
            CacheNode target = clusterClient.getRoutingTarget(key);
            printLine(color(ANSI_DIM, "\"") + color(ANSI_YELLOW, key) + color(ANSI_DIM, "\"")
                    + " → "
                    + color(ANSI_CYAN, target.getId())
                    + color(ANSI_DIM, " (" + target.getAddress() + ")"));
        } catch (Exception e) {
            printErr(e.getMessage());
        }
        return true;
    }

    /**
     * ADDNODE <id> <host> <port>
     *
     * Adds a new server node to the cluster at runtime.
     * Updates the consistent hash ring — future routing includes the new node.
     * Keys previously owned by neighboring nodes may now route to this node.
     *
     * Example:
     *   > addnode node-4 localhost 6382
     *   ✓ Added node [node-4] at localhost:6382
     *   Cluster now has 4 nodes.
     */
    private boolean handleAddNode(String[] args) {
        if (args.length < 3) {
            printErr("Usage: addnode <id> <host> <port>");
            return true;
        }

        String nodeId = args[0];
        String host   = args[1];
        int    port;
        try {
            port = Integer.parseInt(args[2]);
        } catch (NumberFormatException e) {
            printErr("Port must be an integer, got: " + args[2]);
            return true;
        }

        try {
            clusterClient.addServer(nodeId, host, port);
            printLine(color(ANSI_GREEN, "✓ Added node [" + nodeId + "] at " + host + ":" + port));
            printLine(color(ANSI_DIM,
                    "Cluster now has " + clusterClient.getNodeCount() + " node(s)."));

            // Update the prompt to reflect new node count.
            prompt = color(ANSI_BOLD, "jcache")
                    + color(ANSI_DIM, "[cluster:" + clusterClient.getNodeCount() + " nodes]")
                    + color(ANSI_GREEN, "> ") + ANSI_RESET;

        } catch (IOException e) {
            printErr("Failed to add node [" + nodeId + "]: " + e.getMessage());
        }
        return true;
    }

    /**
     * REMOVENODE <id>
     *
     * Removes a server node from the cluster at runtime.
     * Keys that were on this node now route to the next clockwise node.
     * Data on the removed node is NOT migrated — it becomes inaccessible.
     *
     * Example:
     *   > removenode node-4
     *   ✓ Removed node [node-4].
     *   Cluster now has 3 nodes.
     */
    private boolean handleRemoveNode(String[] args) {
        if (args.length < 1) {
            printErr("Usage: removenode <id>");
            return true;
        }

        String nodeId = args[0];

        // Confirm before removing — data on the node becomes inaccessible.
        out.print(color(ANSI_YELLOW,
                "Remove node [" + nodeId + "]? Keys on this node become inaccessible. "
                        + "Type 'yes' to confirm: "));
        out.flush();

        String confirm;
        try {
            confirm = new BufferedReader(new InputStreamReader(System.in)).readLine();
        } catch (IOException e) {
            printErr("Could not read confirmation.");
            return true;
        }

        if (!"yes".equalsIgnoreCase(confirm == null ? "" : confirm.trim())) {
            printLine(color(ANSI_DIM, "Removal cancelled."));
            return true;
        }

        clusterClient.removeServer(nodeId);
        printLine(color(ANSI_GREEN, "✓ Removed node [" + nodeId + "]."));
        printLine(color(ANSI_DIM,
                "Cluster now has " + clusterClient.getNodeCount() + " node(s)."));

        // Update prompt.
        prompt = color(ANSI_BOLD, "jcache")
                + color(ANSI_DIM, "[cluster:" + clusterClient.getNodeCount() + " nodes]")
                + color(ANSI_GREEN, "> ") + ANSI_RESET;

        return true;
    }

    // =========================================================================
    // Command handlers — utility
    // =========================================================================

    /**
     * HISTORY
     *
     * Prints the last N commands entered in this session.
     * Not persistent across sessions.
     *
     * Example:
     *   > history
     *     1  put name Alice
     *     2  get name
     *     3  stats
     */
    private boolean handleHistory(String[] args) {
        if (history.isEmpty()) {
            printLine(color(ANSI_DIM, "(no history yet)"));
            return true;
        }

        printLine("");
        int i = 1;
        for (String entry : history) {
            printLine(String.format("  %3d  %s", i++, entry));
        }
        printLine("");
        return true;
    }

    /**
     * CLEAR
     *
     * Clears the terminal screen using ANSI escape sequences.
     * Works on Unix/macOS terminals. On Windows, prints 50 blank lines instead.
     */
    private boolean handleClear(String[] args) {
        if (colorEnabled) {
            // ANSI: move cursor to top-left and clear screen.
            out.print("\033[H\033[2J");
            out.flush();
        } else {
            // Fallback: print blank lines to push content off screen.
            for (int i = 0; i < 50; i++) out.println();
        }
        return true;
    }

    /**
     * HELP
     *
     * Prints all available commands with their usage.
     * Groups commands by category for readability.
     */
    private boolean handleHelp(String[] args) {
        printLine("");
        printLine(color(ANSI_BOLD, "  JCache CLI — Available Commands"));
        printLine(color(ANSI_DIM,  "  ─────────────────────────────────────────────────"));
        printLine("");

        printLine(color(ANSI_BOLD, "  Cache Operations:"));
        printHelpRow("get <key>",              "Retrieve a value. Returns (nil) on miss.");
        printHelpRow("put <key> <val> [ttl]",  "Store a value. ttl=seconds (0=no expiry).");
        printHelpRow("delete <key>",           "Remove a key. Idempotent.");
        printLine("");

        printLine(color(ANSI_BOLD, "  Server:"));
        printHelpRow("ping",                   "Check server health.");
        printHelpRow("flush",                  "Delete ALL keys (prompts for confirmation).");
        printHelpRow("stats",                  "Show server/cluster metrics.");
        printLine("");

        if (clusterMode) {
            printLine(color(ANSI_BOLD, "  Cluster (cluster mode only):"));
            printHelpRow("nodes",                  "List all cluster nodes.");
            printHelpRow("distribution",            "Show key distribution % per node.");
            printHelpRow("route <key>",             "Show which node a key routes to.");
            printHelpRow("addnode <id> <host> <port>", "Add a node to the cluster.");
            printHelpRow("removenode <id>",         "Remove a node from the cluster.");
            printLine("");
        }

        printLine(color(ANSI_BOLD, "  Utility:"));
        printHelpRow("history",               "Show command history for this session.");
        printHelpRow("clear",                 "Clear the terminal screen.");
        printHelpRow("help",                  "Show this help message.");
        printHelpRow("exit / quit",           "Close connection and exit.");
        printLine("");

        printLine(color(ANSI_DIM,
                "  Aliases: set=put, del=delete, quit=exit"));
        printLine("");
        return true;
    }

    /**
     * EXIT / QUIT
     *
     * Sends QUIT to the server (single-node), closes connections, exits.
     * Returns false to signal the REPL loop to stop.
     */
    private boolean handleExit(String[] args) {
        printLine(color(ANSI_DIM, "Bye!"));
        return false; // signals REPL to exit
    }

    // =========================================================================
    // Cleanup
    // =========================================================================

    /**
     * Closes client connections cleanly.
     * Called from main() in a finally block.
     */
    private void disconnect() {
        if (singleClient != null) {
            try { singleClient.close(); } catch (Exception ignored) {}
        }
        if (clusterClient != null) {
            try { clusterClient.close(); } catch (Exception ignored) {}
        }
    }

    // =========================================================================
    // Output helpers
    // =========================================================================

    /** Prints a line to the output stream. */
    private void printLine(String line) {
        out.println(line);
    }

    /** Prints an error in red to the error stream. */
    private void printErr(String message) {
        err.println(color(ANSI_RED, "(error) ") + message);
    }

    /**
     * Prints a stats table row with aligned key and value columns.
     * Skips rows where value is null (stat not reported by this server version).
     */
    private void printStatRow(String label, String value) {
        if (value == null || value.isEmpty()) return;
        out.printf("  │  %-18s  %s%n",
                color(ANSI_DIM, label + ":"),
                color(ANSI_GREEN, value));
    }

    /**
     * Prints a help row with command and description aligned.
     */
    private void printHelpRow(String command, String description) {
        out.printf("    %-30s %s%n",
                color(ANSI_CYAN, command),
                color(ANSI_DIM, description));
    }

    /**
     * Applies an ANSI color code to text if color is enabled.
     * If color is disabled (piped output, --no-color), returns text unchanged.
     *
     * @param ansiCode One of the ANSI_* constants defined at the top.
     * @param text     The text to color.
     * @return Colored text if enabled, plain text otherwise.
     */
    private String color(String ansiCode, String text) {
        if (!colorEnabled) return text;
        return ansiCode + text + ANSI_RESET;
    }

    /**
     * Returns the value if non-null, or "--" if null.
     * Used in stats display where some fields may be absent.
     */
    private String nvl(String value) {
        return value != null ? value : "--";
    }

    // =========================================================================
    // Input helpers
    // =========================================================================

    /**
     * Tokenizes a command line, respecting double-quoted strings.
     *
     * Rules:
     *   - Tokens are separated by whitespace.
     *   - Text inside double quotes is one token (can contain spaces).
     *   - Quotes are stripped from the result.
     *
     * Examples:
     *   'put key "hello world" 60'  → ["put", "key", "hello world", "60"]
     *   'get name'                  → ["get", "name"]
     *   'put k "val"'               → ["put", "k", "val"]
     *
     * This allows values with spaces without breaking the put command parser.
     *
     * @param line The raw input line.
     * @return Array of tokens with quotes stripped.
     */
    private String[] tokenize(String line) {
        List<String> tokens  = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inQuotes     = false;

        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);

            if (c == '"') {
                // Toggle quote mode — don't include the quote character.
                inQuotes = !inQuotes;

            } else if (c == ' ' && !inQuotes) {
                // Space outside quotes — end of token.
                if (current.length() > 0) {
                    tokens.add(current.toString());
                    current.setLength(0);
                }

            } else {
                // Regular character — append to current token.
                current.append(c);
            }
        }

        // Add last token if any.
        if (current.length() > 0) {
            tokens.add(current.toString());
        }

        return tokens.toArray(new String[0]);
    }

    /**
     * Adds a command to the history deque, capping at MAX_HISTORY entries.
     * Oldest entries are dropped from the front when the cap is reached.
     *
     * @param command The raw command line (before tokenization).
     */
    private void addToHistory(String command) {
        if (history.size() >= MAX_HISTORY) {
            history.pollFirst(); // remove oldest
        }
        history.addLast(command);
    }

    // =========================================================================
    // Formatting helpers
    // =========================================================================

    /**
     * Formats a TTL in seconds into a human-readable duration string.
     *
     * Examples:
     *   60      → "1 minute"
     *   3600    → "1 hour"
     *   86400   → "1 day"
     *   90      → "1 minute 30 seconds"
     *   45      → "45 seconds"
     *
     * @param ttlSeconds TTL in seconds.
     * @return Human-readable duration string.
     */
    private String formatTTL(long ttlSeconds) {
        if (ttlSeconds <= 0) return "no expiry";

        long days    = ttlSeconds / 86400;
        long hours   = (ttlSeconds % 86400) / 3600;
        long minutes = (ttlSeconds % 3600) / 60;
        long seconds = ttlSeconds % 60;

        StringBuilder sb = new StringBuilder();
        if (days    > 0) sb.append(days).append("d ");
        if (hours   > 0) sb.append(hours).append("h ");
        if (minutes > 0) sb.append(minutes).append("m ");
        if (seconds > 0) sb.append(seconds).append("s");

        return sb.toString().trim();
    }

    /**
     * Formats an uptime string from the server stats.
     * Server reports uptime in milliseconds: "uptime:12345ms"
     * We strip the "ms" suffix and format as a duration.
     *
     * @param uptimeStr The raw uptime value from stats, e.g. "12345ms" or "12345".
     * @return Formatted duration string, or the raw value if parsing fails.
     */
    private String formatUptime(String uptimeStr) {
        if (uptimeStr == null) return null;
        try {
            // Strip trailing "ms" if present.
            String numStr = uptimeStr.endsWith("ms")
                    ? uptimeStr.substring(0, uptimeStr.length() - 2)
                    : uptimeStr;
            long   ms      = Long.parseLong(numStr.trim());
            return formatTTL(ms / 1000) + " (" + ms + "ms)";
        } catch (NumberFormatException e) {
            return uptimeStr; // return raw if unparseable
        }
    }

    /**
     * Returns true if the string is a valid non-negative long integer.
     * Used to detect whether the last PUT argument is a TTL.
     *
     * @param s The string to check.
     * @return true if s parses as a non-negative long.
     */
    private boolean isNonNegativeLong(String s) {
        if (s == null || s.isEmpty()) return false;
        try {
            return Long.parseLong(s) >= 0;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    // =========================================================================
    // Usage
    // =========================================================================

    /** Prints usage instructions and exits with code 1. */
    private static void printUsageAndExit() {
        System.err.println();
        System.err.println("  JCache CLI");
        System.err.println();
        System.err.println("  Single-node mode:");
        System.err.println("    java -jar cache-cli.jar <host> <port> [--no-color]");
        System.err.println("    java -jar cache-cli.jar localhost 6379");
        System.err.println();
        System.err.println("  Cluster mode:");
        System.err.println("    java -jar cache-cli.jar --cluster <id:host:port> [<id:host:port>...] [--no-color]");
        System.err.println("    java -jar cache-cli.jar --cluster node-1:localhost:6379 node-2:localhost:6380");
        System.err.println();
        System.err.println("  Options:");
        System.err.println("    --no-color    Disable ANSI color output (useful for piped output)");
        System.err.println();
        System.exit(1);
    }
}