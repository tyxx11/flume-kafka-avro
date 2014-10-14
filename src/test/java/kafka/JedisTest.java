package kafka;

import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * User: willstan
 * Date: 10/10/14
 * Time: 10:13 AM
 */
public class JedisTest {
    @Test
    public void test(){
        /*Jedis jedis = new Jedis("10.10.0.5", 6379);
        jedis.select(1);
        jedis.set("test","test");
        jedis.close();*/
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxIdle(10);
        config.setMaxTotal(100);
        config.setMaxWaitMillis(10000);
        config.setTestOnBorrow(true);
        JedisPool jedisPool = new JedisPool(config, "10.10.0.5", 6379);
        System.out.print("---" + jedisPool.getNumActive());
        Jedis jedis = jedisPool.getResource();
        jedis.select(5);
        if (!jedis.exists("tt")){
            jedis.set("test", "test123");
        }
        jedisPool.returnResource(jedis);
    }

    @Test
    public void md5Test(){
        String s = "sssssasdasdasdasd";
        System.out.println(parseStrToMd5L32(s));
    }

    /**
     * @param str
     * @return
     * @Date: 2014-10-10
     * @Author: wills.tan
     * @Description:  32位小写MD5
     */
    public static String parseStrToMd5L32(String str){
        String reStr = null;
        try {
            MessageDigest md5 = MessageDigest.getInstance("MD5");
            byte[] bytes = md5.digest(str.getBytes());
            StringBuffer stringBuffer = new StringBuffer();
            for (byte b : bytes){
                int bt = b&0xff;
                if (bt < 16){
                    stringBuffer.append(0);
                }
                stringBuffer.append(Integer.toHexString(bt));
            }
            reStr = stringBuffer.toString();
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        return reStr;
    }
}
