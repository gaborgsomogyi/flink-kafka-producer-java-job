/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.apple.flink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.datagen.DataGeneratorSource;
import org.apache.flink.streaming.api.functions.source.datagen.RandomGenerator;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Optional;
import java.util.Properties;

public class FlinkKafkaProducerJavaJob {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkKafkaProducerJavaJob.class);

    public static void main(String[] args) throws Exception {
        LOG.info("Starting Kafka producer job");

        Options options = new Options();
        options.addRequiredOption("b", "broker", true, "Kafka brokers");
        options.addRequiredOption("t", "topic", true, "Kafka topic");

        CommandLineParser parser = new DefaultParser();
        try {
            LOG.info("Parsing command line");
            CommandLine commandLine = parser.parse(options, args);

            var bootstrapServer = commandLine.getOptionValue("b");
            var topic = commandLine.getOptionValue("t");

            var adminProperties = new Properties();
            adminProperties.setProperty(
                    AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
            try (var adminClient = AdminClient.create(adminProperties)) {
                if (adminClient.listTopics().names().get().contains(topic)) {
                    LOG.info("Topic {} already exists, deleting", topic);
                    adminClient.deleteTopics(Collections.singletonList(topic)).all().get();
                    LOG.info("OK");
                }
                LOG.info("Creating topic: {}", topic);
                adminClient
                        .createTopics(
                                Collections.singletonList(
                                        new NewTopic(topic, Optional.empty(), Optional.empty())))
                        .all()
                        .get();
                LOG.info("OK");
            } catch (Exception e) {
                LOG.warn("Exception during topic re-creation: ", e);
            }

            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

            DataStream<String> stream =
                    env.addSource(
                                    new DataGeneratorSource<>(
                                            RandomGenerator.stringGenerator(8), 1, null))
                            .returns(String.class)
                            .uid("datagen-source-uid")
                            .name("datagen-source");

            KafkaSink<String> sink =
                    KafkaSink.<String>builder()
                            .setBootstrapServers(bootstrapServer)
                            .setRecordSerializer(
                                    KafkaRecordSerializationSchema.builder()
                                            .setTopic(topic)
                                            .setValueSerializationSchema(new SimpleStringSchema())
                                            .build())
                            .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                            .build();

            stream.sinkTo(sink).uid("kafka-sink-uid").name("kafka-sink");

            env.execute("flink-kafka-producer");
        } catch (ParseException e) {
            LOG.error("Unable to parse command line options: " + e.getMessage());
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp(FlinkKafkaProducerJavaJob.class.getCanonicalName(), options);
        }

        LOG.info("Exiting Kafka producer job");
    }
}
