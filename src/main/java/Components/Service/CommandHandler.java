package Components.Service;

import Components.Infra.Client;
import Components.Infra.ConnectionPool;
import Components.Infra.Slave;
import Components.Repository.Store;
import Components.Repository.Value;
import Components.Server.RedisConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Base64;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.logging.Level;
import java.util.logging.Logger;

@Component
public class CommandHandler {
    private static final String emptyRdbFile = "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog==";
    private static final Logger logger = Logger.getLogger(CommandHandler.class.getName());
    @Autowired
    public RespSerializer respSerializer;
    @Autowired
    public Store store;
    @Autowired
    public RedisConfig redisConfig;
    @Autowired
    public ConnectionPool connectionPool;
    public String ping(String[] command){
        if(command.length > 1){
            return respSerializer.serializeBulkString(command[1]);
        }
        return "+PONG\r\n";
    }
    public String echo(String[] command){
        if(command.length > 1){
            return respSerializer.serializeBulkString(command[1]);
        }
        return "-ERR wrong number of arguments for 'echo' command\r\n";
    }
    public String set(String[] command){
        try{
            if (command.length < 3) {
                return "-ERR wrong number of arguments for 'set' command\r\n";
            }
            String key = command[1];
            String value = command[2];

            int pxIndex = -1;
            int exIndex = -1;
            for(int i = 3; i < command.length; i++){
                if("px".equalsIgnoreCase(command[i])){
                    pxIndex = i;
                } else if("ex".equalsIgnoreCase(command[i])){
                    exIndex = i;
                }
            }

            if(pxIndex > -1 && pxIndex + 1 < command.length){
                int delta = Integer.parseInt(command[pxIndex + 1]);
                return store.set(key, value, delta);
            } else if(exIndex > -1 && exIndex + 1 < command.length){
                int delta = Integer.parseInt(command[exIndex + 1]) * 1000;
                return store.set(key, value, delta);
            } else {
                return store.set(key, value);
            }
        }catch (Exception e){
            logger.log(Level.SEVERE, e.getMessage());
            return "$-1\r\n";
        }
    }
    public String get(String[] command){
        try{
            if (command.length < 2) {
                return "-ERR wrong number of arguments for 'get' command\r\n";
            }
            String key = command[1];
            return store.get(key);
        } catch (Exception e) {
            logger.log(Level.SEVERE, e.getMessage());
            return "$-1\r\n";
        }
    }
    public String info(String[] command){
        boolean hasReplication = false;
        if(command.length <= 1){
            hasReplication = true;
        } else {
            for(int i = 1; i < command.length; i++){
                if("replication".equalsIgnoreCase(command[i])){
                    hasReplication = true;
                    break;
                }
            }
        }
        if(hasReplication){
            String role = "role:"+redisConfig.getRole();
            String masterReplId = "master_replid:"+redisConfig.getMasterReplId();
            String masterReplOffset = "master_repl_offset:"+redisConfig.getMasterReplOffset();

            String []info = new String[]{role, masterReplId, masterReplOffset};
            String replicationData = String.join("\r\n", info);

            return respSerializer.serializeBulkString(replicationData);
        }
        return respSerializer.serializeBulkString("");
    }
    public String replconf(String[] command, Client client) {
        if(command.length < 2){
            return "+OK\r\n";
        }
        String subCmd = command[1].toUpperCase();
        switch (subCmd){
            case "GETACK":
                String [] replconfAck = new String[]{"REPLCONF", "ACK", String.valueOf(redisConfig.getMasterReplOffset())};
                return respSerializer.respArray(replconfAck);
            case "ACK":
                if(command.length >= 3){
                    int ackResponse = Integer.parseInt(command[2]);
                    connectionPool.slaveAck(client, ackResponse);
                }
                return "";
            case "LISTENING-PORT":
                connectionPool.removeClient(client);
                Slave s = new Slave(client);
                connectionPool.addSlave(s);
                return "+OK\r\n";

            case "CAPA":
                Slave slave = null;
                for(Slave ss: connectionPool.getSlaves()){
                    if(ss.connection != null && ss.connection.equals(client)){
                        slave = ss;
                        break;
                    }
                }
                if(slave != null){
                    for(int i = 2; i < command.length; i++){
                        if(!"capa".equalsIgnoreCase(command[i])){
                            slave.capabilities.add(command[i]);
                        }
                    }
                }
                return "+OK\r\n";
        }

        return "+OK\r\n";
    }
    public byte[] concatenate(byte[] a, byte[] b){
        byte[] result = new byte[a.length +b.length];
        System.arraycopy(a, 0, result, 0, a.length);
        System.arraycopy(b, 0, result, a.length, b.length);
        return result;
    }
    public ResponseDto psync(String[] command) {
        String replicationIdMaster = command.length > 1 ? command[1] : "?";
        String replicationOffSetMaster = command.length > 2 ? command[2] : "-1";

        if(replicationIdMaster.equals("?") && replicationOffSetMaster.equals("-1")){
            String replicationId = redisConfig.getMasterReplId();
            long replicationOffset = redisConfig.getMasterReplOffset();
            String res = "+FULLRESYNC "+ replicationId +" "+replicationOffset+"\r\n";

            byte[] rdbFileData = Base64.getDecoder().decode(emptyRdbFile);
            String length = String.valueOf(rdbFileData.length);
            String fullResyncHeader = "$"+ length +"\r\n";
            byte[] header = fullResyncHeader.getBytes(java.nio.charset.StandardCharsets.UTF_8);

            return new ResponseDto(res, concatenate(header, rdbFileData));
        }else{
            return new ResponseDto("-ERR Options not supported yet.\r\n");
        }
    }
    public String wait(String[] command, Instant start) {
        int required = Integer.parseInt(command[1]);
        long timeoutMs = Long.parseLong(command[2]);

        int currentBytes = connectionPool.bytesSentToSlaves.get();
        if (currentBytes == 0) {
            int count = connectionPool.getSlaves().size();
            return respSerializer.respInteger(count);
        }

        String[] getackArr = new String[] { "REPLCONF", "GETACK", "*" };
        String getack = respSerializer.respArray(getackArr);
        byte[] getackBytes = getack.getBytes(java.nio.charset.StandardCharsets.UTF_8);

        connectionPool.bytesSentToSlaves.addAndGet(getackBytes.length);
        int targetOffset = connectionPool.bytesSentToSlaves.get();

        for (Slave slave : connectionPool.getSlaves()) {
            CompletableFuture.runAsync(() -> {
                try {
                    slave.send(getackBytes);
                } catch (IOException e) {
                    logger.log(Level.WARNING, "Error sending GETACK to slave: " + e.getMessage());
                }
            });
        }

        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            long count = connectionPool.getSlaves().stream()
                    .filter(sl -> sl.lastAckOffset >= targetOffset)
                    .count();
            if (count >= required) {
                return respSerializer.respInteger((int) count);
            }
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }

        long count = connectionPool.getSlaves().stream()
                .filter(sl -> sl.lastAckOffset >= targetOffset)
                .count();
        return respSerializer.respInteger((int) count);
    }
    public String incr(String[] command) {
        if(command.length < 2){
            return "-ERR wrong number of arguments for 'incr' command\r\n";
        }
        return store.incr(command[1]);
    }
    public BiFunction<String[], Map<String, Value>, String> getTransactionCommandCacheApplier(){
        final Store localStore = this.store;
        final RespSerializer localSerializer = this.respSerializer;
        return (String[] command, Map<String, Value> map)->{
            String res = "";
            String cmd = command[0].toUpperCase();
            switch (cmd) {
                case "SET":
                    res = handleSetCommandTransactional(command, map, localSerializer, localStore);
                    break;
                case "GET":
                    res = handleGetCommandTransactional(command, map, localSerializer, localStore);
                    break;
                case "INCR":
                    res = handleIncrCommandTransactional(command, map, localSerializer, localStore);
                    break;
                case "DEL":
                    res = handleDelCommandTransactional(command, map, localSerializer, localStore);
                    break;
                default:
                    res = "-ERR unknown command '"+command[0]+"'\r\n";
                    break;
            }
            return res;
        };
    }

    private String handleDelCommandTransactional(String[] command, Map<String, Value> map, RespSerializer localSerializer, Store localStore) {
        String key = command[1];
        Value valueToUse;
        Value cachedValue = map.getOrDefault(key, null);
        if(cachedValue==null){
            Value storeValue = localStore.getValue(key);
            if(storeValue == null){
                return ":0\r\n";
            }else{
                valueToUse = new Value(storeValue.val, storeValue.created, storeValue.expiry);
            }
        }else{
            if (cachedValue.isDeletedInTransaction) {
                return ":0\r\n";
            }
            valueToUse = cachedValue;
        }
        valueToUse.isDeletedInTransaction = true;
        map.put(key, valueToUse);
        return ":1\r\n";
    }

    private String handleIncrCommandTransactional(
            String[] command,
            Map<String, Value> map,
            RespSerializer localSerializer,
            Store localStore) {
        try{
            String key = command[1];
            Value valueToUse;
            Value cachedValue = map.getOrDefault(key, null);
            if(cachedValue == null || cachedValue.isDeletedInTransaction){
                Value storeValue = (cachedValue == null) ? localStore.getValue(key) : null;
                if(storeValue == null) {
                    valueToUse = new Value("0", LocalDateTime.now(), LocalDateTime.MAX);
                }else{
                    valueToUse = new Value(storeValue.val, storeValue.created, storeValue.expiry);
                }
            }else{
                valueToUse = cachedValue;
            }
            int val = Integer.parseInt(valueToUse.val);
            val++;
            valueToUse.val = String.valueOf(val);
            valueToUse.isDeletedInTransaction = false;
            map.put(key, valueToUse);
            return respSerializer.respInteger(val);
        } catch (Exception e) {
            return "-ERR value is not an integer or out of range\r\n";
        }
    }
    private String handleGetCommandTransactional(
            String[] command,
            Map<String, Value> map,
            RespSerializer localSerializer,
            Store localStore) {
        String key = command[1];
        Value cachedValue = map.getOrDefault(key, null);
        if(cachedValue != null){
            if(cachedValue.isDeletedInTransaction){
                return "$-1\r\n";
            }
            return respSerializer.serializeBulkString(cachedValue.val);
        }
        Value storeValue = localStore.getValue(key);
        if(storeValue == null){
            return "$-1\r\n";
        }
        Value valueToUse = new Value(storeValue.val, storeValue.created, storeValue.expiry);
        map.put(key, valueToUse);
        return respSerializer.serializeBulkString(valueToUse.val);
    }
    private String handleSetCommandTransactional(
            String[] command,
            Map<String, Value> map,
            RespSerializer localSerializer,
            Store localStore) {
        String key = command[1];
        Value newValue = new Value(command[2], LocalDateTime.now(), LocalDateTime.MAX);
        map.put(key, newValue);
        return "+OK\r\n";
    }
}
