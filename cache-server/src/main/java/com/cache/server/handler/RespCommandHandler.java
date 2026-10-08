package com.cache.server.handler;

import com.cache.api.Cache;
import com.cache.common.protocol.RespProtocolException;
import com.cache.common.protocol.RespValue;
import com.cache.persistence.PersistenceManager;
import com.cache.server.ServerConfig;
import com.cache.server.metrics.ServerMetrics;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.DecoderException;

import java.util.List;
import java.util.Locale;
import java.util.function.LongSupplier;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Executes RESP requests, so {@code redis-cli} and Redis client libraries can
 * use the server. Commands and replies follow Redis: {@code GET} of a missing
 * key is a null bulk string, {@code DEL} returns how many keys were removed,
 * {@code TTL} returns {@code -2} for a missing key.
 *
 * <p>Supported: {@code PING [message]}, {@code ECHO}, {@code GET},
 * {@code SET key value [EX seconds | PX milliseconds]}, {@code SETEX},
 * {@code DEL key [key ...]}, {@code EXISTS key [key ...]}, {@code EXPIRE},
 * {@code TTL}, {@code PERSIST}, {@code DBSIZE}, {@code FLUSHALL},
 * {@code FLUSHDB}, {@code INFO}, {@code SELECT 0} and {@code QUIT}. The
 * text-protocol verbs {@code PUT}, {@code DELETE}, {@code STATS} and
 * {@code FLUSH} work too. TTLs have one-second resolution, so {@code PX} is
 * rounded up to whole seconds.
 *
 * <p>One instance is created per connection.
 */
public class RespCommandHandler extends SimpleChannelInboundHandler<List<String>> {

    private static final Logger log = Logger.getLogger(RespCommandHandler.class.getName());

    private static final RespValue OK = RespValue.simpleString("OK");
    private static final RespValue PONG = RespValue.simpleString("PONG");

    private final CommandExecutor executor;

    /**
     * Creates a handler.
     *
     * @param cache       the shared cache; TTL commands need a {@link com.cache.ttl.TTLCache}
     * @param metrics     the shared metrics
     * @param config      server configuration
     * @param persistence where writes are logged, or {@code null} if persistence is off
     */
    public RespCommandHandler(Cache<String, String> cache, ServerMetrics metrics, ServerConfig config,
                              PersistenceManager persistence) {
        this(cache, metrics, config, persistence, System::nanoTime);
    }

    RespCommandHandler(Cache<String, String> cache, ServerMetrics metrics, ServerConfig config,
                       PersistenceManager persistence, LongSupplier nanoClock) {
        this.executor = new CommandExecutor(cache, metrics, config, persistence, nanoClock);
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, List<String> args) {
        if (args.isEmpty()) {
            return;
        }
        if (executor.config().isVerbose()) {
            log.info(ctx.channel().remoteAddress() + " > " + String.join(" ", args));
        }

        String verb = args.get(0).toUpperCase(Locale.ROOT);
        if (verb.equals("QUIT")) {
            ctx.writeAndFlush(OK).addListener(ChannelFutureListener.CLOSE);
            return;
        }
        if (!verb.equals("PING") && !executor.tryAcquire()) {
            ctx.writeAndFlush(RespValue.error("ERR rate limit exceeded"));
            return;
        }

        RespValue reply;
        try {
            reply = execute(verb, args);
        } catch (RespCommandException e) {
            reply = error(e.getMessage());
        } catch (UnsupportedOperationException e) {
            reply = error("ERR " + e.getMessage());
        } catch (RuntimeException e) {
            log.log(Level.WARNING, "Command failed: " + verb, e);
            reply = error("ERR " + e.getMessage());
        }
        ctx.writeAndFlush(reply);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        executor.metrics().recordError();
        if (cause instanceof DecoderException && cause.getCause() instanceof RespProtocolException p) {
            // The stream cannot be resynchronised after malformed input; Redis closes the connection too.
            ctx.writeAndFlush(RespValue.error("ERR Protocol error: " + p.getMessage()))
                    .addListener(ChannelFutureListener.CLOSE);
            return;
        }
        log.log(Level.FINE, "Closing " + ctx.channel().remoteAddress() + " after error", cause);
        ctx.close();
    }

    private RespValue execute(String verb, List<String> args) {
        return switch (verb) {
            case "PING" -> {
                arity(args, 1, 2);
                yield args.size() == 1 ? PONG : RespValue.bulkString(args.get(1));
            }
            case "ECHO" -> {
                arity(args, 2, 2);
                yield RespValue.bulkString(args.get(1));
            }
            case "GET" -> {
                arity(args, 2, 2);
                String value = executor.get(args.get(1));
                yield value == null ? RespValue.nullBulk() : RespValue.bulkString(value);
            }
            case "SET", "PUT" -> {
                arity(args, 3, 5);
                executor.put(args.get(1), args.get(2), setTtl(args));
                yield OK;
            }
            case "SETEX" -> {
                arity(args, 4, 4);
                long seconds = parseLong(args.get(2));
                if (seconds <= 0) {
                    throw new RespCommandException("ERR invalid expire time in 'setex' command");
                }
                executor.put(args.get(1), args.get(3), seconds);
                yield OK;
            }
            case "DEL", "DELETE" -> {
                arity(args, 2, Integer.MAX_VALUE);
                long removed = 0;
                for (String key : args.subList(1, args.size())) {
                    if (executor.delete(key)) {
                        removed++;
                    }
                }
                yield RespValue.integer(removed);
            }
            case "EXISTS" -> {
                arity(args, 2, Integer.MAX_VALUE);
                long found = 0;
                for (String key : args.subList(1, args.size())) {
                    if (executor.exists(key)) {
                        found++;
                    }
                }
                yield RespValue.integer(found);
            }
            case "EXPIRE" -> {
                arity(args, 3, 3);
                long seconds = parseLong(args.get(2));
                if (seconds < 0) {
                    // Redis deletes the key for a negative TTL.
                    yield RespValue.integer(executor.delete(args.get(1)) ? 1 : 0);
                }
                yield RespValue.integer(executor.expire(args.get(1), seconds) ? 1 : 0);
            }
            case "TTL" -> {
                arity(args, 2, 2);
                yield RespValue.integer(executor.ttl(args.get(1)));
            }
            case "PERSIST" -> {
                arity(args, 2, 2);
                yield RespValue.integer(executor.persist(args.get(1)) == CommandExecutor.KeyResult.CHANGED ? 1 : 0);
            }
            case "DBSIZE" -> {
                arity(args, 1, 1);
                yield RespValue.integer(executor.size());
            }
            case "FLUSHALL", "FLUSHDB", "FLUSH" -> {
                arity(args, 1, 2);
                executor.flush();
                yield OK;
            }
            case "SELECT" -> {
                arity(args, 2, 2);
                if (!args.get(1).equals("0")) {
                    throw new RespCommandException("ERR DB index is out of range");
                }
                yield OK;
            }
            case "INFO" -> {
                arity(args, 1, 2);
                yield RespValue.bulkString("# Stats\r\n" + String.join("\r\n", executor.stats().split(" ")) + "\r\n");
            }
            case "STATS" -> {
                arity(args, 1, 1);
                yield RespValue.bulkString(executor.stats());
            }
            default -> throw new RespCommandException("ERR unknown command '" + args.get(0) + "'");
        };
    }

    /** Reads the options after {@code SET key value}; returns the TTL in seconds, 0 for none. */
    private static long setTtl(List<String> args) {
        if (args.size() == 3) {
            return 0;
        }
        String option = args.get(3).toUpperCase(Locale.ROOT);
        if (!option.equals("EX") && !option.equals("PX")) {
            throw new RespCommandException("ERR SET option '" + args.get(3) + "' is not supported");
        }
        if (args.size() != 5) {
            throw new RespCommandException("ERR syntax error");
        }
        long amount = parseLong(args.get(4));
        if (amount <= 0) {
            throw new RespCommandException("ERR invalid expire time in 'set' command");
        }
        return option.equals("EX") ? amount : (amount + 999) / 1000;
    }

    private static long parseLong(String text) {
        try {
            return Long.parseLong(text);
        } catch (NumberFormatException e) {
            throw new RespCommandException("ERR value is not an integer or out of range");
        }
    }

    private static void arity(List<String> args, int min, int max) {
        if (args.size() < min || args.size() > max) {
            throw new RespCommandException("ERR wrong number of arguments for '"
                    + args.get(0).toLowerCase(Locale.ROOT) + "' command");
        }
    }

    private RespValue error(String message) {
        executor.metrics().recordError();
        return RespValue.error(message.replace('\r', ' ').replace('\n', ' '));
    }

    /** A request the server understood but rejected; the message is the full RESP error text. */
    private static final class RespCommandException extends RuntimeException {
        RespCommandException(String message) {
            super(message, null, false, false);
        }
    }
}
