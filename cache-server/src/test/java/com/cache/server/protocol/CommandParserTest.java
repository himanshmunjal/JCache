package com.cache.server.protocol;

import com.cache.server.protocol.Command.ParsedCommand;
import com.cache.server.protocol.Command.Type;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for CommandParser.
 *
 * TESTING PHILOSOPHY:
 * CommandParser is the boundary between "untrusted client bytes" and
 * "structured server logic". It is the most important class to test
 * thoroughly because every invalid input the real world sends will pass
 * through here first.
 *
 * These 5 tests cover the core contract. You should add tests for:
 *   - All remaining valid commands (DELETE, STATS, FLUSH, PING)
 *   - TTL edge cases (TTL=0, very large TTL, Long.MAX_VALUE)
 *   - Whitespace variants (\t delimiter, leading spaces)
 *   - All ProtocolException.ErrorCode values
 *   - Case insensitivity for all command verbs
 *
 * STRUCTURE:
 * Each test follows the AAA (Arrange-Act-Assert) pattern.
 * No shared mutable state between tests — parser is stateless so
 * a single @BeforeEach instance is fine.
 */
@DisplayName("CommandParser Tests")
class CommandParserTest {

    /**
     * The parser under test. Stateless — one instance works for all tests.
     * Created fresh in @BeforeEach for defensive isolation.
     */
    private CommandParser parser;

    @BeforeEach
    void setUp() {
        parser = new CommandParser();
    }

    // =========================================================================
    // Test 1 — Valid GET command
    // =========================================================================

    /**
     * A correctly formed GET command should parse into a ParsedCommand
     * with type=GET and the correct key.
     *
     * WHY THIS TEST MATTERS:
     * GET is the most frequent command in any cache system (read-heavy workloads).
     * This test verifies the happy path: valid input produces correct output.
     *
     * WHAT WE VERIFY:
     *   - type is Type.GET (not null, not a different type)
     *   - key is exactly "username" (not trimmed differently, not lowercased)
     *   - value is null (GET has no value argument)
     *   - ttlSeconds is 0 (GET has no TTL)
     *   - No exception is thrown
     */
    @Test
    @DisplayName("GET with valid key parses correctly")
    void testParse_validGet_parsesCorrectly() throws ProtocolException {
        // Arrange
        String rawLine = "GET username";

        // Act
        ParsedCommand result = parser.parse(rawLine);

        // Assert
        assertNotNull(result, "ParsedCommand must not be null");
        assertEquals(Type.GET, result.getType(),
                "Command type must be GET");
        assertEquals("username", result.getKey(),
                "Key must be exactly 'username'");
        assertNull(result.getValue(),
                "GET command has no value — must be null");
        assertEquals(0L, result.getTtlSeconds(),
                "GET command has no TTL — must be 0");
    }

    // =========================================================================
    // Test 2 — Valid PUT command with TTL
    // =========================================================================

    /**
     * A PUT command with all three arguments (key, value, TTL) should parse
     * all fields correctly, including TTL as a long.
     *
     * WHY THIS TEST MATTERS:
     * PUT is the most complex command — it has optional arguments and requires
     * TTL parsing (string → long). This test exercises the full PUT path.
     *
     * WHAT WE VERIFY:
     *   - type is Type.PUT
     *   - key is "session_token" (exact, case-preserved)
     *   - value is "abc123xyz" (exact)
     *   - ttlSeconds is 3600L (parsed correctly from "3600")
     *   - hasTTL() returns true
     */
    @Test
    @DisplayName("PUT with key, value, and TTL parses all fields correctly")
    void testParse_validPutWithTTL_parsesAllFields() throws ProtocolException {
        // Arrange
        String rawLine = "PUT session_token abc123xyz 3600";

        // Act
        ParsedCommand result = parser.parse(rawLine);

        // Assert
        assertEquals(Type.PUT,            result.getType(),       "Type must be PUT");
        assertEquals("session_token",     result.getKey(),        "Key must be 'session_token'");
        assertEquals("abc123xyz",         result.getValue(),      "Value must be 'abc123xyz'");
        assertEquals(3600L,               result.getTtlSeconds(), "TTL must be 3600");
        assertTrue(result.hasTTL(),
                "hasTTL() must return true when TTL > 0");
    }

    // =========================================================================
    // Test 3 — Unknown command throws ProtocolException with UNKNOWN_COMMAND
    // =========================================================================

    /**
     * A command verb that doesn't exist in the Command.Type enum should throw
     * a ProtocolException with ErrorCode.UNKNOWN_COMMAND.
     *
     * WHY THIS TEST MATTERS:
     * Unknown commands are the most common client error. The server must:
     *   1. Not crash (no uncaught exception)
     *   2. Return an informative error (not "null pointer exception")
     *   3. Keep the connection alive for the next command
     *
     * This test verifies (1) and (2). (3) is CacheServerHandler's responsibility.
     *
     * WHAT WE VERIFY:
     *   - ProtocolException IS thrown (not silently swallowed)
     *   - ErrorCode is UNKNOWN_COMMAND specifically (not MISSING_ARGUMENT, etc.)
     *   - The exception message mentions the unknown verb "INVALIDCMD"
     */
    @Test
    @DisplayName("Unknown command verb throws ProtocolException with UNKNOWN_COMMAND code")
    void testParse_unknownCommand_throwsProtocolException() {
        // Arrange
        String rawLine = "INVALIDCMD somekey";

        // Act & Assert
        ProtocolException ex = assertThrows(
                ProtocolException.class,
                () -> parser.parse(rawLine),
                "Unknown command must throw ProtocolException"
        );

        assertEquals(ProtocolException.ErrorCode.UNKNOWN_COMMAND, ex.getErrorCode(),
                "Error code must be UNKNOWN_COMMAND");

        assertTrue(ex.getMessage().contains("INVALIDCMD"),
                "Error message should mention the unknown verb 'INVALIDCMD', got: " + ex.getMessage());
    }

    // =========================================================================
    // Test 4 — GET with no key throws ProtocolException with MISSING_ARGUMENT
    // =========================================================================

    /**
     * A GET command with no key argument should throw ProtocolException
     * with ErrorCode.MISSING_ARGUMENT.
     *
     * WHY THIS TEST MATTERS:
     * Missing arguments are common when clients have bugs or when someone
     * is manually testing with telnet and forgets the key. The server must
     * give a clear error explaining what was missing — not "ArrayIndexOutOfBoundsException".
     *
     * WHAT WE VERIFY:
     *   - ProtocolException IS thrown
     *   - ErrorCode is MISSING_ARGUMENT (not UNKNOWN_COMMAND or TOO_MANY_ARGS)
     *   - Exception message mentions GET and the requirement
     */
    @Test
    @DisplayName("GET with no key throws ProtocolException with MISSING_ARGUMENT code")
    void testParse_getWithNoKey_throwsMissingArgument() {
        // Arrange
        String rawLine = "GET";

        // Act & Assert
        ProtocolException ex = assertThrows(
                ProtocolException.class,
                () -> parser.parse(rawLine),
                "GET with no key must throw ProtocolException"
        );

        assertEquals(ProtocolException.ErrorCode.MISSING_ARGUMENT, ex.getErrorCode(),
                "Error code must be MISSING_ARGUMENT");

        // Message should help the client understand what was wrong
        assertTrue(ex.getMessage().contains("GET"),
                "Error message should mention the command 'GET', got: " + ex.getMessage());
    }

    // =========================================================================
    // Test 5 — Command verbs are case-insensitive
    // =========================================================================

    /**
     * Command verbs in any case (lowercase, uppercase, mixed) should be
     * treated identically. "get", "GET", "Get", "gEt" all mean the same thing.
     *
     * WHY THIS TEST MATTERS:
     * Real clients send commands in whatever case their library uses.
     * Redis is case-insensitive for command verbs. Our protocol must match
     * this expectation. If we're case-sensitive, clients have to know our
     * exact casing, which is fragile and undocumented behavior.
     *
     * WHAT WE VERIFY:
     *   - "get username" (all lowercase) parses as Type.GET
     *   - "Get username" (title case) parses as Type.GET
     *   - Keys remain case-sensitive ("Username" != "username")
     *
     * We test lowercase as it's the variant most likely to be broken —
     * Enum.valueOf() is case-sensitive by default, and our parser must
     * explicitly uppercase the verb before lookup.
     */
    @Test
    @DisplayName("Command verbs are case-insensitive — lowercase 'get' parses as GET")
    void testParse_lowercaseVerb_isCaseInsensitive() throws ProtocolException {
        // Arrange
        String rawLineLower = "get mykey";
        String rawLineMixed = "Get mykey";

        // Act
        ParsedCommand resultLower = parser.parse(rawLineLower);
        ParsedCommand resultMixed = parser.parse(rawLineMixed);

        // Assert — both must parse as GET regardless of verb case
        assertEquals(Type.GET, resultLower.getType(),
                "Lowercase 'get' must parse as Type.GET");
        assertEquals(Type.GET, resultMixed.getType(),
                "Mixed-case 'Get' must parse as Type.GET");

        // Keys ARE case-sensitive — "mykey" must not be changed
        assertEquals("mykey", resultLower.getKey(),
                "Key must be preserved exactly as-is (case-sensitive)");
        assertEquals("mykey", resultMixed.getKey(),
                "Key must be preserved exactly as-is (case-sensitive)");
    }
}