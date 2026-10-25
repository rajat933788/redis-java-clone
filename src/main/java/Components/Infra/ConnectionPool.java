package Components.Infra;

import org.springframework.stereotype.Component;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

@Component
public class ConnectionPool {
    private final Set<Client> clients = ConcurrentHashMap.newKeySet();
    private final Set<Slave> slaves = ConcurrentHashMap.newKeySet();
    public int slavesThatAreCaughtUp = 0;
    public final AtomicInteger bytesSentToSlaves = new AtomicInteger(0);

    public ConnectionPool() {
    }

    public void slaveAck(int ackResponse){
        if(this.bytesSentToSlaves.get() <= ackResponse){
            slavesThatAreCaughtUp++;
        }
    }

    public void slaveAck(Client client, int ackResponse){
        if(client != null){
            for(Slave s : slaves){
                if(client.equals(s.connection)){
                    s.lastAckOffset = ackResponse;
                    break;
                }
            }
        }
        slaveAck(ackResponse);
    }

    public Set<Client> getClients() {
        return clients;
    }

    public Set<Slave> getSlaves() {
        return slaves;
    }

    public void addClient(Client client){
        if(client!=null)
            clients.add(client);
    }

    public void addSlave(Slave slave){
        if(slave!=null)
            slaves.add(slave);
    }

    public boolean removeClient(Client client){
        if(client == null) return false;
        return clients.remove(client);
    }

    public boolean removeSlave(Slave slave){
        if(slave == null) return false;
        return slaves.remove(slave);
    }

    public boolean removeSlave(Client client){
        if(client == null) return false;
        return slaves.removeIf(s -> s.connection.equals(client));
    }
}
