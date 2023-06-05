import org.apache.commons.lang3.StringUtils;
import redis.clients.jedis.Jedis;

import java.util.HashMap;

public class Test11RedisSet {
    public static void main(String[] args) {
        String host = "hadoop102";
        int port = 6379;
        Jedis jedis = new Jedis(host, port); // 该方法不报异常，所以下方自己写一下，查看redis是否正常连接
        String ping = jedis.ping();
        if (StringUtils.isNotBlank(ping)) {
            System.out.println("redis connection successfully created!");
        } else {
            System.out.println("redis connection failed!");
        }
        HashMap<String,String> map = new HashMap<>();
        map.put("k1","v1");
        map.put("k2","v2");
        String x = jedis.hmset("x", map);
        System.out.println(x);
    }
}
