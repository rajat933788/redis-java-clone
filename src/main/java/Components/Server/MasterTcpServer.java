package Components.Server;

import Components.Infra.ConnectionPool;
import Components.Infra.Slave;
import Components.Repository.Store;
import Components.Repository.Value;
import Components.Service.CommandHandler;
import Components.Service.RespSerializer;
import Components.Infra.Client;
import Components.Service.ResponseDto;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Instant;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.logging.Level;
import java.util.logging.Logger;

@Component
public class MasterTcpServer {
    private static final Logger logger = Logger.getLogger(MasterTcpServer.class.getName());
    @Autowired
    private RespSerializer respSerializer;
    @Autowired
    private CommandHandler commandHandler;
    @Autowired
    private RedisConfig redisConfig;
    @Autowired
    private ConnectionPool connectionPool;
    @Autowired
    private Store store;
    public void startServer(){
        ServerSocket serverSocket = null;
        int port = redisConfig.getPort();
        try {
            serverSocket = new ServerSocket(port);
            serverSocket.setReuseAddress(true);
            int id = 0;
            while (!serverSocket.isClosed()) {
                Socket clientSocket = serverSocket.accept();
                id++;
                Socket finalClientSocket = clientSocket;

                InputStream inputStream = clientSocket.getInputStream();
                OutputStream outputStream = clientSocket.getOutputStream();

                Client client = new Client(finalClientSocket, inputStream, outputStream, id);
                CompletableFuture.runAsync(() -> {
                    try {
                        handleClient(client);
                    } catch (Exception e) {
                        logger.log(Level.INFO, "Client handler exception: " + e.getMessage());
                    }
                });
            }

        } catch (IOException e) {
            logger.log(Level.SEVERE, e.getMessage());
        } finally {
            try {
                if (serverSocket != null && !serverSocket.isClosed()) {
                    serverSocket.close();
                }
            } catch (IOException e) {
                logger.log(Level.SEVERE, e.getMessage());
            }
        }
    }

    private void handleClient(Client client) throws IOException {
        connectionPool.addClient(client);
        try {
            byte[] buffer = new byte[8192];
            while (!client.socket.isClosed()) {
                int bytesRead = client.inputStream.read(buffer);
                if (bytesRead == -1) {
                    break;
                }
                if (bytesRead > 0) {
                    List<String[]> commands = respSerializer.deserialize(buffer, bytesRead);
                    for (String[] command : commands) {
                        handleCommand(command, client);
                    }
                }
            }
        } catch (IOException e) {
            logger.log(Level.FINE, "Client connection closed: " + e.getMessage());
        } finally {
            try {
                client.socket.close();
            } catch (IOException ignored) {}
            connectionPool.removeClient(client);
            connectionPool.removeSlave(client);
        }
    }

    private void handleCommand(String[] command, Client client) throws IOException {
        if (command == null || command.length == 0) {
            return;
        }
        String cmd = command[0].toUpperCase();
        if (!client.getTransactionalContext()) {
            ResponseDto responseDto = caseHandler(command, client);
            client.send(responseDto);
        } else if ("MULTI".equals(cmd)) {
            client.send("-ERR MULTI calls can not be nested\r\n");
        } else if (!isTransactionalControlCommand(cmd)) {
            addCommandToTransaction(command, client);
        } else {
            transactionController(command, client);
        }
    }

    private void transactionController(String[] command, Client client) throws IOException {
        String cmd = command[0].toUpperCase();
        switch (cmd){
            case "EXEC":
                if(client.commandQueue == null || client.commandQueue.isEmpty()){
                    client.send("*0\r\n");
                    client.endTransaction();
                    return;
                }

                Queue<String[]> commands = new LinkedList<>(client.commandQueue);

                BiFunction<String[], Map<String, Value>, String> transactionCacheApplier = commandHandler.getTransactionCommandCacheApplier();
                store.executeTransaction(client, transactionCacheApplier);

                client.endTransaction();
                while(!commands.isEmpty()){
                    String[] commandToPropagate = commands.poll();
                    String action = commandToPropagate[0].toUpperCase();
                    if ("SET".equals(action) || "INCR".equals(action) || "DEL".equals(action)) {
                        String commandRespString = respSerializer.respArray(commandToPropagate);
                        byte[] toCount = commandRespString.getBytes(java.nio.charset.StandardCharsets.UTF_8);
                        connectionPool.bytesSentToSlaves.addAndGet(toCount.length);
                        CompletableFuture.runAsync(() -> propagate(commandToPropagate));
                    }
                }

                String response = respSerializer.respArray(client.transactionResponse);
                client.send(response);
                break;
            case "DISCARD":
                client.endTransaction();
                client.send("+OK\r\n");
                break;
        }
    }

    private void addCommandToTransaction(String[] command, Client client) throws IOException {
        client.commandQueue.offer(command);
        client.send("+QUEUED\r\n");
    }

    private boolean isTransactionalControlCommand(String command) {
        return switch (command.toUpperCase()) {
            case "EXEC", "DISCARD" -> true;
            default -> false;
        };
    }

    public ResponseDto caseHandler(String[] command, Client client){
        String res = "";
        byte[] data = null;
        String cmd = command[0].toUpperCase();
        switch (cmd){
            case "PING":
                res = commandHandler.ping(command);
                break;
            case "EXEC":
                res = "-ERR EXEC without MULTI\r\n";
                break;
            case "DISCARD":
                res = "-ERR DISCARD without MULTI\r\n";
                break;
            case "MULTI":
                client.beginTransaction();
                res = "+OK\r\n";
                break;
            case "INCR":
                res = commandHandler.incr(command);
                if (res.startsWith(":")) {
                    String commandRespString = respSerializer.respArray(command);
                    byte[] toCount = commandRespString.getBytes(java.nio.charset.StandardCharsets.UTF_8);
                    connectionPool.bytesSentToSlaves.addAndGet(toCount.length);
                    CompletableFuture.runAsync(() -> propagate(command));
                }
                break;
            case "ECHO":
                res = commandHandler.echo(command);
                break;
            case "SET":
                res = commandHandler.set(command);
                String commandRespString = respSerializer.respArray(command);
                byte[] toCount = commandRespString.getBytes(java.nio.charset.StandardCharsets.UTF_8);
                connectionPool.bytesSentToSlaves.addAndGet(toCount.length);
                CompletableFuture.runAsync(() -> propagate(command));
                break;
            case "GET":
                res = commandHandler.get(command);
                break;
            case "INFO":
                res = commandHandler.info(command);
                break;
            case "REPLCONF":
                res = commandHandler.replconf(command, client);
                break;
            case "WAIT":
                Instant start = Instant.now();
                res = commandHandler.wait(command, start);
                break;
            case "PSYNC":
                ResponseDto resDto = commandHandler.psync(command);
                res = resDto.response;
                data = resDto.data;
                break;
            default:
                res = "-ERR unknown command '" + command[0] + "'\r\n";
                break;
        }
        return new ResponseDto(res, data);
    }

    private void propagate(String[] command) {
        String commandRespString = respSerializer.respArray(command);
        byte[] bytes = commandRespString.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        for(Slave slave: connectionPool.getSlaves()){
            try {
                slave.send(bytes);
            } catch (IOException e) {
                logger.log(Level.WARNING, "Failed to send command to slave: " + e.getMessage());
            }
        }
    }
}
