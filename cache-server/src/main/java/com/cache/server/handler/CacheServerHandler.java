package com.cache.server.handler;

import com.cache.api.Cache;
import com.cache.common.protocol.Command;
import com.cache.common.protocol.Command.ParsedCommand;
import com.cache.common.protocol.CommandParser;
import com.cache.common.protocol.ProtocolException;
import com.cache.common.protocol.ResponseEncoder;
import com.cache.persistence.PersistenceManager;
import com.cache.server.ServerConfig;
import com.cache.server.metrics.ServerMetrics;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.TooLongFrameException;

import java.util.function.LongSupplier;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Executes one text-protocol command per line and writes the reply.
 *
 * <p>One instance is created per connection, so Netty calls it from a single
 * I/O thread. The cache, metrics and persistence manager are shared between
 * connections and must be thread-safe.
 *
 * <p>Replies: {@code +OK}, {@code +PONG}, {@code +<value>}, {@code +<seconds>}
 * for TTL, {@code +<stats>}, or {@code -ERR <message>}.
 *
 * <p>When a rate limit is configured, each connection has its own token
 * bucket. Commands over the limit get {@code -ERR rate limit exceeded} and are
 * not run; later commands succeed once tokens refill. {@code PING} and
 * {@code QUIT} are not limited.
 */
public class CacheServerHandler extends SimpleChannelInboundHandler<String> {

    private static final Logger log = Logger.getLogger(CacheServerHandler.class.getName());
    private static final CommandParser PARSER = new CommandParser();

    private final CommandExecutor executor;

    /**
     * Creates a handler without persistence.
     *
     * @param cache   the shared cache
     * @param metrics the shared metrics
     * @param config  server configuration
     */
    public CacheServerHandler(Cache<String, String> cache, ServerMetrics metrics, ServerConfig config) {
        this(cache, metrics, config, null);
    }

    /**
     * Creates a handler.
     *
     * @param cache       the shared cache; TTL commands need a {@link com.cache.ttl.TTLCache}
     * @param metrics     the shared metrics
     * @param config      server configuration
     * @param persistence where writes are logged, or {@code null} if persistence is off
     */
    public CacheServerHandler(Cache<String, String> cache, ServerMetrics metrics, ServerConfig config,
                              PersistenceManager persistence) {
        this(cache, metrics, config, persistence, System::nanoTime);
    }

    CacheServerHandler(Cache<String, String> cache, ServerMetrics metrics, ServerConfig config,
                       PersistenceManager persistence, LongSupplier nanoClock) {
        this.executor = new CommandExecutor(cache, metrics, config, persistence, nanoClock);
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, String line) {
        if (line.isBlank()) {
            return;
        }
        if (executor.config().isVerbose()) {
            log.info(ctx.channel().remoteAddress() + " > " + line);
        }

        ParsedCommand command;
        try {
            command = PARSER.parse(line);
        } catch (ProtocolException e) {
            executor.metrics().recordError();
            ctx.writeAndFlush(ResponseEncoder.error(e.getMessage()));
            return;
        }

        Command.Type type = command.getType();
        if (type != Command.Type.PING && type != Command.Type.QUIT && !executor.tryAcquire()) {
            ctx.writeAndFlush(ResponseEncoder.error("rate limit exceeded"));
            return;
        }

        if (type == Command.Type.QUIT) {
            ctx.writeAndFlush(ResponseEncoder.bye()).addListener(ChannelFutureListener.CLOSE);
            return;
        }

        String reply;
        try {
            reply = execute(command);
        } catch (UnsupportedOperationException e) {
            executor.metrics().recordError();
            reply = ResponseEncoder.error(e.getMessage());
        } catch (RuntimeException e) {
            executor.metrics().recordError();
            log.log(Level.WARNING, "Command failed: " + command, e);
            reply = ResponseEncoder.error(e.getMessage());
        }
        ctx.writeAndFlush(reply);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        executor.metrics().recordError();
        if (cause instanceof TooLongFrameException) {
            // The decoder has already discarded the oversized line; the connection is still usable.
            ctx.writeAndFlush(ResponseEncoder.error("line too long"));
            return;
        }
        log.log(Level.FINE, "Closing " + ctx.channel().remoteAddress() + " after error", cause);
        ctx.close();
    }

    private String execute(ParsedCommand cmd) {
        String key = cmd.getKey();
        return switch (cmd.getType()) {
            case PING -> ResponseEncoder.pong();
            case GET -> {
                String value = executor.get(key);
                yield value != null ? ResponseEncoder.value(value) : ResponseEncoder.keyNotFound();
            }
            case PUT -> {
                executor.put(key, cmd.getValue(), cmd.getTtlSeconds());
                yield ResponseEncoder.ok();
            }
            case DELETE -> {
                executor.delete(key);
                yield ResponseEncoder.ok();
            }
            case EXPIRE -> executor.expire(key, cmd.getTtlSeconds())
                    ? ResponseEncoder.ok() : ResponseEncoder.keyNotFound();
            case TTL -> {
                long ttl = executor.ttl(key);
                yield ttl == -2 ? ResponseEncoder.keyNotFound() : ResponseEncoder.value(Long.toString(ttl));
            }
            case PERSIST -> executor.persist(key) == CommandExecutor.KeyResult.ABSENT
                    ? ResponseEncoder.keyNotFound() : ResponseEncoder.ok();
            case STATS -> ResponseEncoder.value(executor.stats());
            case FLUSH -> {
                executor.flush();
                yield ResponseEncoder.ok();
            }
            case QUIT -> throw new IllegalStateException("QUIT is handled before execute()");
        };
    }
}
