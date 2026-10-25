package Components.Server;

import Components.Infra.ConnectionPool;
import Components.Service.CommandHandler;
import Components.Service.RespSerializer;
import Components.Infra.Client;
import Components.Service.ResponseDto;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.logging.Level;
import java.util.logging.Logger;

@Component
public class SlaveTcpServer {
    private static final Logger logger = Logger.getLogger(SlaveTcpServer.class.getName());
    @Autowired
    private RespSerializer respSerializer;
    @Autowired
    private CommandHandler commandHandler;
    @Autowired
    private RedisConfig redisConfig;
    @Autowired
    private ConnectionPool connectionPool;

    public void startServer(){
        ServerSocket serverSocket = null;
        int port = redisConfig.getPort();

        try {
            serverSocket = new ServerSocket(port);
            serverSocket.setReuseAddress(true);

            CompletableFuture<Void> slaveConnectionFuture = CompletableFuture.runAsync(this::initiateSlavery);
            slaveConnectionFuture.thenRun(() -> System.out.println("Replication completed"));

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

    private void initiateSlavery() {
        try (Socket master = new Socket(redisConfig.getMasterHost(), redisConfig.getMasterPort())) {
            InputStream in = master.getInputStream();
            OutputStream out = master.getOutputStream();

            // Handshake 1: PING
            out.write("*1\r\n$4\r\nPING\r\n".getBytes(StandardCharsets.UTF_8));
            out.flush();
            readSimpleString(in);

            // Handshake 2: REPLCONF listening-port <port>
            String portStr = String.valueOf(redisConfig.getPort());
            String replconfPort = "*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$" +
                    portStr.length() + "\r\n" + portStr + "\r\n";
            out.write(replconfPort.getBytes(StandardCharsets.UTF_8));
            out.flush();
            readSimpleString(in);

            // Handshake 3: REPLCONF capa psync2
            String replconfCapa = "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n";
            out.write(replconfCapa.getBytes(StandardCharsets.UTF_8));
            out.flush();
            readSimpleString(in);

            // Handshake 4: PSYNC ? -1
            String psync = "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n";
            out.write(psync.getBytes(StandardCharsets.UTF_8));
            out.flush();

            // Read +FULLRESYNC ...
            readSimpleString(in);

            // Read RDB file
            readRdbFile(in);

            logger.info("Replication handshake completed.");
            redisConfig.setMasterReplOffset(0L);

            // Process replication stream from master
            long processedOffset = 0L;
            ByteArrayOutputStream cmdBuffer = new ByteArrayOutputStream();

            byte[] buffer = new byte[8192];
            while (!master.isClosed()) {
                int bytesRead = in.read(buffer);
                if (bytesRead == -1) {
                    break;
                }
                cmdBuffer.write(buffer, 0, bytesRead);
                byte[] allBytes = cmdBuffer.toByteArray();

                int parsedUpTo = 0;
                List<ParsedCommand> parsedCommands = parseCommandsFromReplicationStream(allBytes);

                for (ParsedCommand pc : parsedCommands) {
                    handleReplicationCommand(pc.command, out, processedOffset + pc.bytesConsumed);
                    processedOffset += pc.bytesConsumed;
                    redisConfig.setMasterReplOffset(processedOffset);
                    parsedUpTo += pc.bytesConsumed;
                }

                if (parsedUpTo > 0) {
                    cmdBuffer.reset();
                    if (parsedUpTo < allBytes.length) {
                        cmdBuffer.write(allBytes, parsedUpTo, allBytes.length - parsedUpTo);
                    }
                }
            }

        } catch (Exception e) {
            logger.log(Level.SEVERE, "Master connection error: " + e.getMessage(), e);
        }
    }

    private String readSimpleString(InputStream in) throws IOException {
        StringBuilder sb = new StringBuilder();
        int b;
        while ((b = in.read()) != -1) {
            sb.append((char) b);
            if (sb.length() >= 2 && sb.charAt(sb.length() - 2) == '\r' && sb.charAt(sb.length() - 1) == '\n') {
                break;
            }
        }
        return sb.toString();
    }

    private byte[] readRdbFile(InputStream in) throws IOException {
        StringBuilder sb = new StringBuilder();
        int b;
        while ((b = in.read()) != -1) {
            sb.append((char) b);
            if (sb.length() >= 2 && sb.charAt(sb.length() - 2) == '\r' && sb.charAt(sb.length() - 1) == '\n') {
                break;
            }
        }
        String header = sb.toString().trim();
        if (!header.startsWith("$")) {
            throw new IOException("Invalid RDB header: " + header);
        }
        int length = Integer.parseInt(header.substring(1));
        byte[] rdb = new byte[length];
        int totalRead = 0;
        while (totalRead < length) {
            int read = in.read(rdb, totalRead, length - totalRead);
            if (read == -1) {
                throw new IOException("Unexpected EOF while reading RDB file");
            }
            totalRead += read;
        }
        return rdb;
    }

    private static class ParsedCommand {
        String[] command;
        int bytesConsumed;

        ParsedCommand(String[] command, int bytesConsumed) {
            this.command = command;
            this.bytesConsumed = bytesConsumed;
        }
    }

    private List<ParsedCommand> parseCommandsFromReplicationStream(byte[] data) {
        List<ParsedCommand> list = new ArrayList<>();
        int pos = 0;
        while (pos < data.length) {
            while (pos < data.length && (data[pos] == '\r' || data[pos] == '\n' || data[pos] == ' ')) {
                pos++;
            }
            if (pos >= data.length) {
                break;
            }
            if (data[pos] != '*') {
                pos++;
                continue;
            }

            int startPos = pos;
            int crlf = findCRLF(data, pos);
            if (crlf == -1) {
                break;
            }
            String countStr = new String(data, pos + 1, crlf - (pos + 1), StandardCharsets.UTF_8);
            int count;
            try {
                count = Integer.parseInt(countStr.trim());
            } catch (NumberFormatException e) {
                break;
            }
            pos = crlf + 2;

            boolean fullCommandParsed = true;
            String[] cmd = new String[count];

            for (int i = 0; i < count; i++) {
                if (pos >= data.length || data[pos] != '$') {
                    fullCommandParsed = false;
                    break;
                }
                int strLenCrlf = findCRLF(data, pos);
                if (strLenCrlf == -1) {
                    fullCommandParsed = false;
                    break;
                }
                String lenStr = new String(data, pos + 1, strLenCrlf - (pos + 1), StandardCharsets.UTF_8);
                int strLen;
                try {
                    strLen = Integer.parseInt(lenStr.trim());
                } catch (NumberFormatException e) {
                    fullCommandParsed = false;
                    break;
                }
                pos = strLenCrlf + 2;

                if (pos + strLen + 2 > data.length) {
                    fullCommandParsed = false;
                    break;
                }

                cmd[i] = new String(data, pos, strLen, StandardCharsets.UTF_8);
                pos += strLen + 2; // skip data and \r\n
            }

            if (fullCommandParsed) {
                int bytesConsumed = pos - startPos;
                list.add(new ParsedCommand(cmd, bytesConsumed));
            } else {
                break;
            }
        }
        return list;
    }

    private int findCRLF(byte[] data, int start) {
        for (int i = start; i < data.length - 1; i++) {
            if (data[i] == '\r' && data[i + 1] == '\n') {
                return i;
            }
        }
        return -1;
    }

    private void handleReplicationCommand(String[] command, OutputStream masterOut, long currentOffset) throws IOException {
        if (command == null || command.length == 0) return;
        String cmd = command[0].toUpperCase();
        switch (cmd) {
            case "SET":
                commandHandler.set(command);
                break;
            case "INCR":
                commandHandler.incr(command);
                break;
            case "REPLCONF":
                if (command.length >= 2 && "GETACK".equalsIgnoreCase(command[1])) {
                    String offsetStr = String.valueOf(currentOffset);
                    String ackResp = "*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$" +
                            offsetStr.length() + "\r\n" + offsetStr + "\r\n";
                    masterOut.write(ackResp.getBytes(StandardCharsets.UTF_8));
                    masterOut.flush();
                }
                break;
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
            logger.log(Level.FINE, "Client disconnected: " + e.getMessage());
        } finally {
            try {
                client.socket.close();
            } catch (IOException ignored) {}
            connectionPool.removeClient(client);
            connectionPool.removeSlave(client);
        }
    }

    private void handleCommand(String[] command, Client client) throws IOException {
        if (command == null || command.length == 0) return;
        String res = "";
        byte[] data = null;
        String cmd = command[0].toUpperCase();
        switch (cmd){
            case "PING":
                res = commandHandler.ping(command);
                break;
            case "ECHO":
                res = commandHandler.echo(command);
                break;
            case "SET":
            case "INCR":
                res = "-READONLY You can't write against a replica.\r\n";
                break;
            case "GET":
                res = commandHandler.get(command);
                break;
            case "INFO":
                res = commandHandler.info(command);
                break;
            case "PSYNC":
                ResponseDto resDto = commandHandler.psync(command);
                res = resDto.response;
                data = resDto.data;
                break;
            case "WAIT":
                Instant start = Instant.now();
                res = commandHandler.wait(command, start);
                break;
            default:
                res = "-ERR unknown command '" + command[0] + "'\r\n";
                break;
        }
        client.send(res, data);
    }
}
