/*******************************************************************************
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *  
 * http://www.apache.org/licenses/LICENSE-2.0
 *  
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *******************************************************************************/
package com.zcloud.analytics.flume;

import com.zcloud.snake.common.AvroResolver;
import com.zcloud.snake.common.AvroUtils;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.conf.ConfigurationException;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;

/**
 * A Sink of Kafka which get events from channels and publish to Kafka. I use
 * this in our company production environment which can hit 100k messages per
 * second. <tt>zk.connect: </tt> the zookeeper ip kafka use.
 * <p>
 * <tt>topic: </tt> the topic to read from kafka.
 * <p>
 * <tt>batchSize: </tt> send serveral messages in one request to kafka.
 * <p>
 * <tt>producer.type: </tt> type of producer of kafka, async or sync is
 * available.<o> <tt>serializer.class: </tt>{@kafka.serializer.StringEncoder
 * 
 * 
 * }
 */
public class KafkaSink extends AbstractSink implements Configurable {
	private static final Logger log = LoggerFactory.getLogger(KafkaSink.class);
	private String topic;
	private Producer<String, byte[]> producer;

    private Jedis jedis;

    @Override
	public Status process() throws EventDeliveryException {
		Channel channel = getChannel();
		Transaction tx = channel.getTransaction();
		try {
			tx.begin();
			Event event = channel.take();
			if (event == null) {
				tx.commit();
				return Status.READY;

			}
            String messageStr = new String(event.getBody());
            if (messageCheck(messageStr)) { //only if message hasn't be send then produce it.
                //convert string to avro record.
                Schema schema = AvroUtils.generateSchema(topic);
                GenericRecord gr = getGr(messageStr, schema);
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                DatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(schema);
                Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);
                writer.write(gr, encoder);
                encoder.flush();
                out.close();
                KeyedMessage<String, byte[]> keyedMessage = new KeyedMessage<String, byte[]>(topic, out.toByteArray());
                producer.send(keyedMessage);
                log.trace("Message: {}", event.getBody());
            }
            tx.commit();
			return Status.READY;
		} catch (Exception e) {
			try {
				tx.rollback();
				log.error("process error! try to rollback...", e);
				return Status.BACKOFF;
			} catch (Exception e2) {
				log.error("Rollback Exception:{}", e2);
			}		
			log.error("KafkaSink Exception:{}", e);
			return Status.BACKOFF;
		} finally {
			tx.close();
		}
	}

    @Override
	public void configure(Context context) {
		topic = context.getString("topic");
		if (topic == null) {
			throw new ConfigurationException("Kafka topic must be specified.");
		}
		producer = KafkaSinkUtil.getProducer(context);


        jedis = KafkaSinkUtil.getJedis(context);
	}

    public GenericRecord getGr(String strMessage, Schema schema) throws IOException {
        GenericRecord gr = AvroUtils.generateGR(topic);
        List<Schema.Field> fieldList = schema.getFields();
        String[] messages = strMessage.split("\t");
        int i = 0;
		for(String msg : messages){
			if (msg != null && !msg.equals("-")) {
				Schema.Field field = fieldList.get(i);
				Schema.Type type = getFirstType(field);
				if (type == Schema.Type.LONG) {
					gr.put(field.name(), Long.parseLong(msg));
				}else{
					gr.put(field.name(), msg);
				}
			}
			i++;
		}
        return gr;
    }

	@Override
	public synchronized void start() {
		super.start();
	}

	@Override
	public synchronized void stop() {
		producer.close();
		super.stop();
	}

	private static Schema.Type getFirstType(Schema.Field field) {
		Schema.Type type = field.schema().getType();
		if (type == Schema.Type.UNION) {
			return field.schema().getTypes().get(0).getType();
		} else {
			return type;
		}
	}


    public boolean messageCheck(String message){
        if(message != null){
            String md5Str = parseStrToMd5L32(message);
            try {
                jedis.select(5);
                if (!jedis.exists(md5Str)) {
                    jedis.set(md5Str, "0");
                    //retain 1 day
                    jedis.expire(md5Str, 60 * 60 * 24);
                    return true;
                }
            } catch (Exception e){
                log.error("jedis error! ", e);
            }
        }
        return false;
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
