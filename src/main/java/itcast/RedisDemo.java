package itcast;

import org.junit.Before;
import org.junit.Test;

public class RedisDemo {
    private JedisPool jedisPool;
    @Before
    public void init(){
        JedisPoolConfig config=new JedisPoolConfig();
        config.setMaxIdle(10);//设置最大等待空闲连接数
        config.setMinIdle(5);//设置最小等待空闲连接数
        config.setMaxTotal(50);//允许最大的连接数
        config.setMaxWaitMillis(3000);//设置最大等待连接时间

        jedisPool=new JedisPool(config,"hadoop01");
    }
    @Test
    public void operateString(){
        Jedis jedis=jedisPool.getResource();
        jedis.set("itcast1","value1");
        String itcast1 = jedis.get("itcast1");
        System.out.println(itcast1);
    }
}
