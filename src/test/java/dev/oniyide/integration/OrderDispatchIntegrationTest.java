package dev.oniyide.integration;

import static java.util.UUID.randomUUID;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.equalTo;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;

import dev.oniyide.DispatchConfiguration;
import dev.oniyide.message.DispatchPreparing;
import dev.oniyide.message.OrderCreated;
import dev.oniyide.message.OrderDispatched;
import dev.oniyide.util.TestEventData;
import lombok.extern.slf4j.Slf4j;


@Slf4j
@SpringBootTest(classes = {DispatchConfiguration.class})
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@ActiveProfiles("test")
@EmbeddedKafka(controlledShutdown = true)
public class OrderDispatchIntegrationTest
{
    private static final String ORDER_CREATED_TOPIC ="order.created";
    private static final String ORDER_DISPATCHED_TOPIC ="order.dispatched";
    private static final String DISPATCH_TRACKING_TOPIC ="dispatch.tracking";

    @Autowired
    private KafkaTestListener kafkaTestListener;

    @Autowired
    private KafkaTemplate kafkaTemplate;

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    @Autowired
    private KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;

    @Configuration
    static class TestConfig{
        @Bean
        public KafkaTestListener testListener(){
            return new KafkaTestListener();
        }
    }

    public static class KafkaTestListener{
        AtomicInteger dispatchPreparingCounter = new AtomicInteger(0);
        AtomicInteger orderDispatchCounter = new AtomicInteger(0);

        @KafkaListener(groupId = "kafkaIntegrationTest", topics = DISPATCH_TRACKING_TOPIC)
        void receiveDispatchPreparing(@Header(KafkaHeaders.RECEIVED_KEY) String key, @Payload DispatchPreparing dispatchPreparing){
            log.debug("Received payload: {}", dispatchPreparing);
            Assertions.assertThat(key).isNotNull();
            Assertions.assertThat(dispatchPreparing).isNotNull();
            dispatchPreparingCounter.incrementAndGet();
        }
        @KafkaListener(groupId = "kafkaIntegrationTest", topics = ORDER_DISPATCHED_TOPIC)
        void receiveOrderDispatched(@Header(KafkaHeaders.RECEIVED_KEY) String key, @Payload OrderDispatched orderDispatched){
            log.debug("Received payload: {}", orderDispatched);
            Assertions.assertThat(key).isNotNull();
            Assertions.assertThat(orderDispatched).isNotNull();
            orderDispatchCounter.incrementAndGet();
        }

    }


    @BeforeEach
    void setUp(){
        kafkaTestListener.dispatchPreparingCounter.set(0);
        kafkaTestListener.orderDispatchCounter.set(0);

        kafkaListenerEndpointRegistry.getListenerContainers().forEach(container ->{
            ContainerTestUtils.waitForAssignment(container, embeddedKafkaBroker.getPartitionsPerTopic());
        });
    }

    @Test
    void testOrderDispatchFlow() throws Exception{
        OrderCreated orderCreated = TestEventData.buildOrderCreatedEvent(randomUUID(), "my-item");
        sendMessage(ORDER_CREATED_TOPIC, randomUUID().toString(), orderCreated);

        await().atMost(3, TimeUnit.SECONDS).pollDelay(100, TimeUnit.MILLISECONDS)
            .until(kafkaTestListener.dispatchPreparingCounter::get, equalTo(1));
        await().atMost(1, TimeUnit.SECONDS).pollDelay(100, TimeUnit.MILLISECONDS)
            .until(kafkaTestListener.orderDispatchCounter::get, equalTo(1));
        
    }

    private void sendMessage(String topic, String key, final Object data) throws ExecutionException, InterruptedException
    {
        kafkaTemplate.send(
            MessageBuilder.withPayload(data)
                .setHeader(KafkaHeaders.TOPIC, topic)
                .setHeader(KafkaHeaders.KEY, key)
                .build()).get();
    }
}
