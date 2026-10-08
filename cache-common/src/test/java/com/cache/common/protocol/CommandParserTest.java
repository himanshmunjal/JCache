package com.cache.common.protocol;

import com.cache.common.protocol.Command.ParsedCommand;
import com.cache.common.protocol.Command.Type;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("CommandParser")
class CommandParserTest {
    private final CommandParser parser = new CommandParser();

    @Test
    @DisplayName("GET with a key")
    void get() throws ProtocolException {
        ParsedCommand cmd = parser.parse("GET username");

        assertEquals(Type.GET, cmd.getType());
        assertEquals("username", cmd.getKey());
        assertNull(cmd.getValue());
        assertEquals(0L, cmd.getTtlSeconds());
    }

    @Test
    @DisplayName("PUT with key, value and TTL")
    void putWithTtl() throws ProtocolException {
        ParsedCommand cmd = parser.parse("PUT session_token abc123xyz 3600");

        assertEquals(Type.PUT, cmd.getType());
        assertEquals("session_token", cmd.getKey());
        assertEquals("abc123xyz", cmd.getValue());
        assertEquals(3600L, cmd.getTtlSeconds());
        assertTrue(cmd.hasTTL());
    }

    @Test
    @DisplayName("PUT keeps spaces inside the value")
    void putMultiWordValue() throws ProtocolException {
        ParsedCommand cmd = parser.parse("PUT greeting hello   big world");

        assertEquals("hello   big world", cmd.getValue());
        assertEquals(0L, cmd.getTtlSeconds());
    }

    @Test
    @DisplayName("PUT treats a trailing number as the TTL only when there is a value before it")
    void putNumericValue() throws ProtocolException {
        assertEquals("42", parser.parse("PUT answer 42").getValue());

        ParsedCommand withTtl = parser.parse("PUT order item 42 0");
        assertEquals("item 42", withTtl.getValue());
        assertEquals(0L, withTtl.getTtlSeconds());
    }

    @Test
    @DisplayName("PUT key that also appears in the verb is parsed by position")
    void putKeyMatchingVerbText() throws ProtocolException {
        ParsedCommand cmd = parser.parse("PUT P hello");

        assertEquals("P", cmd.getKey());
        assertEquals("hello", cmd.getValue());
    }

    @ParameterizedTest(name = "{0} -> {1}")
    @CsvSource({"SET k v, PUT", "DEL k, DELETE", "EXIT, QUIT", "set k v, PUT"})
    @DisplayName("Aliases map to their command")
    void aliases(String line, Type expected) throws ProtocolException {
        assertEquals(expected, parser.parse(line).getType());
    }

    @Test
    @DisplayName("TTL with one argument queries, with two it sets the TTL")
    void ttlForms() throws ProtocolException {
        assertEquals(Type.TTL, parser.parse("TTL k").getType());

        ParsedCommand set = parser.parse("TTL k 30");
        assertEquals(Type.EXPIRE, set.getType());
        assertEquals(30L, set.getTtlSeconds());
    }

    @Test
    @DisplayName("EXPIRE rejects a non-numeric TTL")
    void expireInvalidTtl() {
        ProtocolException ex = assertThrows(ProtocolException.class, () -> parser.parse("EXPIRE k soon"));
        assertEquals(ProtocolException.ErrorCode.INVALID_TTL, ex.getErrorCode());
    }

    @Test
    @DisplayName("Unknown verb is rejected and named in the message")
    void unknownCommand() {
        ProtocolException ex = assertThrows(ProtocolException.class, () -> parser.parse("INVALIDCMD somekey"));

        assertEquals(ProtocolException.ErrorCode.UNKNOWN_COMMAND, ex.getErrorCode());
        assertTrue(ex.getMessage().contains("INVALIDCMD"), ex.getMessage());
    }

    @Test
    @DisplayName("GET without a key is rejected")
    void getMissingKey() {
        ProtocolException ex = assertThrows(ProtocolException.class, () -> parser.parse("GET"));

        assertEquals(ProtocolException.ErrorCode.MISSING_ARGUMENT, ex.getErrorCode());
        assertTrue(ex.getMessage().contains("GET"), ex.getMessage());
    }

    @Test
    @DisplayName("GET with two keys is rejected")
    void getTooManyArgs() {
        ProtocolException ex = assertThrows(ProtocolException.class, () -> parser.parse("GET my key"));
        assertEquals(ProtocolException.ErrorCode.TOO_MANY_ARGS, ex.getErrorCode());
    }

    @Test
    @DisplayName("Blank line is rejected")
    void emptyLine() {
        ProtocolException ex = assertThrows(ProtocolException.class, () -> parser.parse("   "));
        assertEquals(ProtocolException.ErrorCode.EMPTY_COMMAND, ex.getErrorCode());
    }

    @Test
    @DisplayName("Verbs are case-insensitive, keys are not")
    void caseInsensitiveVerb() throws ProtocolException {
        assertEquals(Type.GET, parser.parse("get mykey").getType());
        assertEquals(Type.GET, parser.parse("Get MyKey").getType());
        assertEquals("MyKey", parser.parse("Get MyKey").getKey());
    }
}
