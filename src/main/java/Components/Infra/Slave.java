package Components.Infra;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;

public class Slave {
    public Client connection;
    public List<String> capabilities;
    public volatile long lastAckOffset = 0;

    public Slave(Client client){
        this.connection = client;
        this.capabilities = new CopyOnWriteArrayList<>();
    }

    public synchronized void send(byte[] bytes) throws IOException {
        if(bytes != null && connection != null && connection.outputStream != null){
            this.connection.outputStream.write(bytes);
            this.connection.outputStream.flush();
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Slave slave = (Slave) o;
        return Objects.equals(connection, slave.connection);
    }

    @Override
    public int hashCode() {
        return Objects.hash(connection);
    }
}
