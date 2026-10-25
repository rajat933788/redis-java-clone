package Components.Infra;

import Components.Service.ResponseDto;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

public class Client {
    public Socket socket;
    public InputStream inputStream;
    public OutputStream outputStream;
    public int id;

    private boolean transactionalContext;
    public Queue<String[]> commandQueue;
    public List<String> transactionResponse; // the strings will be in resp format we just need to make a resp array out of it


    public boolean getTransactionalContext(){return transactionalContext;}

    public boolean beginTransaction(){
        if(transactionalContext)
            return false;
        transactionalContext = true;
        transactionResponse = new ArrayList<>();
        commandQueue = new LinkedList<>();
        return transactionalContext;
    }

    public void endTransaction(){
        commandQueue = null;
        transactionalContext=false;
    }

    public Client(Socket socket,
                  InputStream inputStream,
                  OutputStream outputStream,
                  int id){
        this.socket = socket;
        this.inputStream = inputStream;
        this.outputStream = outputStream;
        this.id = id;
    }

    public synchronized void send(String res, byte[] data) throws IOException {
        if(res !=null && !res.isEmpty())
            outputStream.write(res.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        if(data!=null)
            outputStream.write(data);
        outputStream.flush();
    }

    public synchronized void send(ResponseDto res) throws IOException {
        if(res != null){
            if(res.response !=null && !res.response.isEmpty())
                outputStream.write(res.response.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            if(res.data!=null)
                outputStream.write(res.data);
            outputStream.flush();
        }
    }

    public synchronized void send(byte[] data) throws IOException {
        if(data!=null){
            outputStream.write(data);
            outputStream.flush();
        }
    }

    public synchronized void send(String data) throws IOException {
        if(data!=null && !data.isEmpty()){
            outputStream.write(data.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            outputStream.flush();
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Client client = (Client) o;
        return id == client.id && java.util.Objects.equals(socket, client.socket);
    }

    @Override
    public int hashCode() {
        return java.util.Objects.hash(id, socket);
    }
}
