import Components.Server.MasterTcpServer;
import Components.Server.RedisConfig;
import Components.Server.SlaveTcpServer;
import Config.AppConfig;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;


public class Main {
    public static void main(String[] args){
        AnnotationConfigApplicationContext context =
              new AnnotationConfigApplicationContext(AppConfig.class);
        MasterTcpServer master = context.getBean(MasterTcpServer.class);
        SlaveTcpServer slave = context.getBean(SlaveTcpServer.class);
        RedisConfig redisConfig = context.getBean(RedisConfig.class);
        int port = 6379;
        redisConfig.setPort(port);
        redisConfig.setRole("master");
        for(int i = 0; i < args.length; i++){
            if("--port".equalsIgnoreCase(args[i]) && i + 1 < args.length){
                port = Integer.parseInt(args[++i]);
                redisConfig.setPort(port);
            } else if("--replicaof".equalsIgnoreCase(args[i]) && i + 1 < args.length){
                redisConfig.setRole("slave");
                String nextArg = args[++i];
                if(nextArg.contains(" ")){
                    String[] parts = nextArg.split("\\s+");
                    redisConfig.setMasterHost(parts[0]);
                    redisConfig.setMasterPort(Integer.parseInt(parts[1]));
                } else if(i + 1 < args.length && !args[i + 1].startsWith("--")){
                    redisConfig.setMasterHost(nextArg);
                    redisConfig.setMasterPort(Integer.parseInt(args[++i]));
                }
            }
        }

        if("slave".equalsIgnoreCase(redisConfig.getRole())){
            slave.startServer();
        }else{
            master.startServer();
        }
    }
}
