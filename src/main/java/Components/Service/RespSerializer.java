package Components.Service;

import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

@Component
public class RespSerializer {
    private static final Logger logger = Logger.getLogger(RespSerializer.class.getName());

    public String serializeBulkString(String s){
        if (s == null) {
            return "$-1\r\n";
        }
        int length = s.getBytes(StandardCharsets.UTF_8).length;
        return "$" + length + "\r\n" + s + "\r\n";
    }

    public int getParts(char[] dataArr, int i, String[] subArray){
        int j = 0;
        while(i < dataArr.length && j < subArray.length){
            while (i < dataArr.length && (dataArr[i] == '\r' || dataArr[i] == '\n' || dataArr[i] == ' ')) {
                i++;
            }
            if(i >= dataArr.length || dataArr[i] == '\u0000') {
                break;
            }
            if(dataArr[i] == '$'){
                i++;
                StringBuilder partLength = new StringBuilder();
                boolean isNegative = false;
                if(i < dataArr.length && dataArr[i] == '-') {
                    isNegative = true;
                    i++;
                }
                while(i < dataArr.length && Character.isDigit(dataArr[i])){
                    partLength.append(dataArr[i]);
                    i++;
                }
                while (i < dataArr.length && (dataArr[i] == '\r' || dataArr[i] == '\n')) {
                    i++;
                }
                if (partLength.length() == 0) {
                    continue;
                }
                int len = Integer.parseInt(partLength.toString());
                if(isNegative || len < 0){
                    subArray[j++] = null;
                } else {
                    StringBuilder part = new StringBuilder();
                    for(int k = 0; k < len && i < dataArr.length; k++){
                        part.append(dataArr[i++]);
                    }
                    while (i < dataArr.length && (dataArr[i] == '\r' || dataArr[i] == '\n')) {
                        i++;
                    }
                    subArray[j++] = part.toString();
                }
            } else {
                i++;
            }
        }
        return i;
    }

    public List<String[]> deseralize(byte[] command){
        if (command == null || command.length == 0) {
            return new ArrayList<>();
        }
        return deserialize(command, command.length);
    }

    public List<String[]> deserialize(byte[] command, int length){
        try{
            if (command == null || length <= 0) {
                return new ArrayList<>();
            }
            String data = new String(command, 0, length, StandardCharsets.UTF_8);
            char[] dataArr = data.toCharArray();
            List<String[]> res = new ArrayList<>();

            int i = 0;
            while(i < dataArr.length){
                char curr = dataArr[i];

                if(curr == '\u0000'){
                    break;
                }

                if(curr == '\r' || curr == '\n' || curr == ' '){
                    i++;
                    continue;
                }

                if(curr == '*'){
                    i++;
                    StringBuilder arrLen = new StringBuilder();
                    while(i < dataArr.length && Character.isDigit(dataArr[i])){
                        arrLen.append(dataArr[i++]);
                    }
                    while(i < dataArr.length && (dataArr[i] == '\r' || dataArr[i] == '\n')){
                        i++;
                    }
                    if(arrLen.length() == 0){
                        continue;
                    }
                    int totalCount = Integer.parseInt(arrLen.toString());
                    if(totalCount <= 0){
                        continue;
                    }

                    if(i < dataArr.length && dataArr[i] == '*'){
                        for(int t = 0; t < totalCount && i < dataArr.length; t++){
                            while(i < dataArr.length && (dataArr[i] == '\r' || dataArr[i] == '\n' || dataArr[i] == ' ')){
                                i++;
                            }
                            if(i < dataArr.length && dataArr[i] == '*'){
                                i++;
                                StringBuilder nestedLen = new StringBuilder();
                                while(i < dataArr.length && Character.isDigit(dataArr[i])){
                                    nestedLen.append(dataArr[i++]);
                                }
                                while(i < dataArr.length && (dataArr[i] == '\r' || dataArr[i] == '\n')){
                                    i++;
                                }
                                if(nestedLen.length() > 0){
                                    int nLen = Integer.parseInt(nestedLen.toString());
                                    String[] subArray = new String[nLen];
                                    i = getParts(dataArr, i, subArray);
                                    res.add(subArray);
                                }
                            }
                        }
                    } else {
                        String[] subArray = new String[totalCount];
                        i = getParts(dataArr, i, subArray);
                        res.add(subArray);
                    }
                } else {
                    StringBuilder line = new StringBuilder();
                    while(i < dataArr.length && dataArr[i] != '\r' && dataArr[i] != '\n' && dataArr[i] != '\u0000'){
                        line.append(dataArr[i++]);
                    }
                    while(i < dataArr.length && (dataArr[i] == '\r' || dataArr[i] == '\n')){
                        i++;
                    }
                    String trimmed = line.toString().trim();
                    if(!trimmed.isEmpty()){
                        String[] parts = trimmed.split("\\s+");
                        res.add(parts);
                    }
                }
            }
            return res;
        } catch (Exception e) {
            logger.log(Level.SEVERE, e.getMessage());
        }
        return new ArrayList<>();
    }

    public String respInteger(int i){
        return ":" + i + "\r\n";
    }

    public String respArray(String[] command) {
        if (command == null) {
            return "*0\r\n";
        }
        StringBuilder sb = new StringBuilder();
        sb.append("*").append(command.length).append("\r\n");
        for(String s: command){
            if (s == null) {
                sb.append("$-1\r\n");
            } else {
                sb.append("$").append(s.getBytes(StandardCharsets.UTF_8).length).append("\r\n");
                sb.append(s).append("\r\n");
            }
        }
        return sb.toString();
    }

    public String respArray(List<String> command) {
        if (command == null) {
            return "*0\r\n";
        }
        StringBuilder sb = new StringBuilder();
        sb.append("*").append(command.size()).append("\r\n");
        for(String s: command){
            sb.append(s);
        }
        return sb.toString();
    }

    public String[] parseArray(String[] parts) {
        if (parts == null || parts.length == 0) {
            return new String[0];
        }
        String lenStr = parts[0];
        if (lenStr.startsWith("*")) {
            lenStr = lenStr.substring(1);
        }
        int length = Integer.parseInt(lenStr);
        String[] command = new String[length];
        if (parts.length > 2) {
            command[0] = parts[2];
        }
        int idx = 1;
        for(int i = 4; i < parts.length && idx < length; i += 2){
            command[idx++] = parts[i];
        }
        return command;
    }
}
