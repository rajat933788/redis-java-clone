package Components.Service;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class RespSerializerTest {
    private final RespSerializer respSerializer = new RespSerializer();

    @Test
    public void testDeserializePing(){
        String ping = "*1\r\n$4\r\nPING\r\n";
        List<String[]> commands = respSerializer.deseralize(ping.getBytes(StandardCharsets.UTF_8));

        assertEquals(1, commands.size());
        assertEquals(1, commands.get(0).length);
        assertEquals("PING", commands.get(0)[0]);
    }

    @Test
    public void testMultipleCommands(){
        String multipleCommands = "*2\r\n*3\r\n$3\r\nset\r\n$3\r\nkey\r\n$5\r\nvalue\r\n*3\r\n$3\r\nset\r\n$3\r\nkey\r\n$5\r\nvalue\u0000";
        List<String[]> commands = respSerializer.deseralize(multipleCommands.getBytes(StandardCharsets.UTF_8));

        assertEquals(2, commands.size());
        assertEquals(3, commands.get(0).length);
        assertEquals(3, commands.get(1).length);

        assertEquals("set", commands.get(0)[0]);
        assertEquals("key", commands.get(0)[1]);
        assertEquals("value", commands.get(0)[2]);

        assertEquals("set", commands.get(1)[0]);
        assertEquals("key", commands.get(1)[1]);
        assertEquals("value", commands.get(1)[2]);
    }

    @Test
    public void testConsecutivePipelinedCommands() {
        String stream = "*1\r\n$4\r\nPING\r\n*2\r\n$4\r\nECHO\r\n$5\r\nhello\r\n";
        List<String[]> commands = respSerializer.deseralize(stream.getBytes(StandardCharsets.UTF_8));

        assertEquals(2, commands.size());
        assertArrayEquals(new String[]{"PING"}, commands.get(0));
        assertArrayEquals(new String[]{"ECHO", "hello"}, commands.get(1));
    }

    @Test
    public void testInlineCommand() {
        String inline = "PING\r\n";
        List<String[]> commands = respSerializer.deseralize(inline.getBytes(StandardCharsets.UTF_8));

        assertEquals(1, commands.size());
        assertArrayEquals(new String[]{"PING"}, commands.get(0));
    }

    @Test
    public void testSerializeBulkString() {
        assertEquals("$5\r\nhello\r\n", respSerializer.serializeBulkString("hello"));
        assertEquals("$-1\r\n", respSerializer.serializeBulkString(null));
    }

    @Test
    public void testRespArray() {
        String[] cmd = new String[]{"SET", "key", "val"};
        String expected = "*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$3\r\nval\r\n";
        assertEquals(expected, respSerializer.respArray(cmd));
    }

    @Test
    public void testParseArray() {
        String[] parts = new String[]{"*2", "$4", "ECHO", "$2", "hi"};
        String[] parsed = respSerializer.parseArray(parts);
        assertEquals(2, parsed.length);
        assertEquals("ECHO", parsed[0]);
        assertEquals("hi", parsed[1]);
    }
}