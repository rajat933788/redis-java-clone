package Components.Service;

import Components.Infra.Client;
import Components.Repository.Store;
import Components.Server.RedisConfig;
import Config.AppConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.net.Socket;
import java.util.LinkedList;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest(classes = AppConfig.class)
public class CommandHandlerTest {

    @Autowired
    private CommandHandler commandHandler;

    @Autowired
    private Store store;

    @Autowired
    private RedisConfig redisConfig;

    @Autowired
    private RespSerializer respSerializer;

    @BeforeEach
    public void setUp() {
        store.map.clear();
        redisConfig.setPort(6379);
        redisConfig.setRole("master");
        redisConfig.setMasterReplOffset(0L);
    }

    @Test
    public void testPingWithoutArgs() {
        String result = commandHandler.ping(new String[]{"PING"});
        assertEquals("+PONG\r\n", result);
    }

    @Test
    public void testPingWithArg() {
        String result = commandHandler.ping(new String[]{"PING", "hello"});
        assertEquals("$5\r\nhello\r\n", result);
    }

    @Test
    public void testEcho() {
        String result = commandHandler.echo(new String[]{"ECHO", "hello world"});
        assertEquals("$11\r\nhello world\r\n", result);
    }

    @Test
    public void testSetAndGet() {
        String setResult = commandHandler.set(new String[]{"SET", "mykey", "myval"});
        assertEquals("+OK\r\n", setResult);

        String getResult = commandHandler.get(new String[]{"GET", "mykey"});
        assertEquals("$5\r\nmyval\r\n", getResult);
    }

    @Test
    public void testGetNonExistentKey() {
        String getResult = commandHandler.get(new String[]{"GET", "nonexistent"});
        assertEquals("$-1\r\n", getResult);
    }

    @Test
    public void testSetWithPxExpiry() throws InterruptedException {
        String setResult = commandHandler.set(new String[]{"SET", "tempKey", "val", "px", "100"});
        assertEquals("+OK\r\n", setResult);

        String getResult = commandHandler.get(new String[]{"GET", "tempKey"});
        assertEquals("$3\r\nval\r\n", getResult);

        Thread.sleep(120);

        String getResultExpired = commandHandler.get(new String[]{"GET", "tempKey"});
        assertEquals("$-1\r\n", getResultExpired);
    }

    @Test
    public void testSetWithExExpiry() throws InterruptedException {
        String setResult = commandHandler.set(new String[]{"SET", "exKey", "val", "ex", "1"});
        assertEquals("+OK\r\n", setResult);

        String getResult = commandHandler.get(new String[]{"GET", "exKey"});
        assertEquals("$3\r\nval\r\n", getResult);
    }

    @Test
    public void testIncrNewKey() {
        String result = commandHandler.incr(new String[]{"INCR", "counter"});
        assertEquals(":1\r\n", result);
    }

    @Test
    public void testIncrExistingKey() {
        commandHandler.set(new String[]{"SET", "counter", "10"});
        String result = commandHandler.incr(new String[]{"INCR", "counter"});
        assertEquals(":11\r\n", result);
    }

    @Test
    public void testIncrNonInteger() {
        commandHandler.set(new String[]{"SET", "counter", "abc"});
        String result = commandHandler.incr(new String[]{"INCR", "counter"});
        assertEquals("-ERR value is not an integer or out of range\r\n", result);
    }

    @Test
    public void testInfoReplication() {
        String result = commandHandler.info(new String[]{"INFO", "replication"});
        assertTrue(result.contains("role:master"));
        assertTrue(result.contains("master_replid:"));
        assertTrue(result.contains("master_repl_offset:0"));
    }

    @Test
    public void testPsync() {
        ResponseDto dto = commandHandler.psync(new String[]{"PSYNC", "?", "-1"});
        assertNotNull(dto);
        assertTrue(dto.response.startsWith("+FULLRESYNC "));
        assertNotNull(dto.data);
        assertTrue(dto.data.length > 0);
    }

    @Test
    public void testTransactionalExecution() {
        Client client = new Client(new Socket(), null, null, 1);
        client.beginTransaction();
        client.commandQueue.offer(new String[]{"SET", "k1", "10"});
        client.commandQueue.offer(new String[]{"INCR", "k1"});
        client.commandQueue.offer(new String[]{"GET", "k1"});

        store.executeTransaction(client, commandHandler.getTransactionCommandCacheApplier());

        assertEquals(3, client.transactionResponse.size());
        assertEquals("+OK\r\n", client.transactionResponse.get(0));
        assertEquals(":11\r\n", client.transactionResponse.get(1));
        assertEquals("$2\r\n11\r\n", client.transactionResponse.get(2));

        // Verify stored value
        assertEquals("$2\r\n11\r\n", store.get("k1"));
    }
}