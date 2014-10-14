#def
agent_log.sources = source1
agent_log.sinks = kafka
agent_log.channels = channel
#souce
agent_log.sources.source1.type = exec
agent_log.sources.source1.command = tail -F /home/wills.log
agent_log.sources.source1.batchSize = 32
agent_log.sources.source1.restart = true
#sink
agent_log.sinks.kafka.type = com.zcloud.analytics.flume.KafkaSink
agent_log.sinks.kafka.metadata.broker.list = 127.0.0.1:9092
agent_log.sinks.kafka.topic = NewUser
agent_log.sinks.kafka.redishost=127.0.0.1
agent_log.sinks.kafka.redisport=6379
agent_log.sinks.kafka.batchsize = 32
agent_log.sinks.kafka.producer.type = async
agent_log.sinks.kafka.serializer.class = kafka.serializer.DefaultEncoder
agent_log.sinks.kafka.request.required.acks = 1
#channel
agent_log.channels.channel.type = memory
agent_log.channels.channel.capacity = 512
agent_log.channels.channel.transactionCapacity = 128
#bind
agent_log.sources.source1.channels = channel
agent_log.sinks.kafka.channel = channel
