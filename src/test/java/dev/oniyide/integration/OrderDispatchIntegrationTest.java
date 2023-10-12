package dev.oniyide.integration;

import static dev.oniyide.integration.WiremockUtils.stubWiremock;
import static java.util.UUID.randomUUID;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.equalTo;

import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.contract.wiremock.AutoConfigureWireMock;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaHandler;
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

import com.github.tomakehurst.wiremock.stubbing.Scenario;

import dev.oniyide.DispatchConfiguration;
import dev.oniyide.message.DispatchCompleted;
import dev.oniyide.message.DispatchPreparing;
import dev.oniyide.message.OrderCreated;
import dev.oniyide.message.OrderDispatched;
import dev.oniyide.util.TestEventData;
import lombok.extern.slf4j.Slf4j;


@Slf4j
@SpringBootTest(classes = {DispatchConfiguration.class})
@AutoConfigureWireMock(port = 0)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@ActiveProfiles("test")
@EmbeddedKafka(controlledShutdown = true)
public class OrderDispatchIntegrationTest
{
    private static final String ORDER_CREATED_TOPIC ="order.created";
    private static final String ORDER_DISPATCHED_TOPIC ="order.dispatched";
    private static final String DISPATCH_TRACKING_TOPIC ="dispatch.tracking";
    private static final String ORDER_CREATED_DLT_TOPIC ="order.created.DLT";

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

    @KafkaListener(groupId = "KafkaIntegrationTest", topics = { DISPATCH_TRACKING_TOPIC, ORDER_DISPATCHED_TOPIC, ORDER_CREATED_DLT_TOPIC})
    public static class KafkaTestListener{
        AtomicInteger dispatchPreparingCounter = new AtomicInteger(0);
        AtomicInteger orderDispatchCounter = new AtomicInteger(0);
        AtomicInteger dispatchCompletedCounter = new AtomicInteger(0);
        AtomicInteger orderCreatedDLTCounter = new AtomicInteger(0);

        @KafkaHandler
        void receiveDispatchPreparing(@Header(KafkaHeaders.RECEIVED_KEY) String key, @Payload DispatchPreparing dispatchPreparing){
            log.debug("Received DispatchPreparing: {} key: {} ", dispatchPreparing, key);
            Assertions.assertThat(key).isNotNull();
            Assertions.assertThat(dispatchPreparing).isNotNull();
            dispatchPreparingCounter.incrementAndGet();
        }

        @KafkaHandler
        void receiveOrderDispatched(@Header(KafkaHeaders.RECEIVED_KEY) String key, @Payload OrderDispatched orderDispatched){
            log.debug("Received OrderDispatched: {} key: {} ", orderDispatched, key);
            Assertions.assertThat(key).isNotNull();
            Assertions.assertThat(orderDispatched).isNotNull();
            orderDispatchCounter.incrementAndGet();
        }

        @KafkaHandler
        void receiveDispatchCompleted(@Header(KafkaHeaders.RECEIVED_KEY) String key, @Payload DispatchCompleted dispatchCompleted){
            log.debug("Received DispatchCompleted: {} key: {} ", dispatchCompleted, key);
            Assertions.assertThat(key).isNotNull();
            Assertions.assertThat(dispatchCompleted).isNotNull();
            dispatchCompletedCounter.incrementAndGet();
        }

        @KafkaHandler
        void receiveOrderCreatedDLT(@Header(KafkaHeaders.RECEIVED_KEY) String key, @Payload OrderCreated orderCreatedDLT){
            log.debug("Received OrderCreated DLT: {} key: {} ", orderCreatedDLT, key);
            Assertions.assertThat(key).isNotNull();
            Assertions.assertThat(orderCreatedDLT).isNotNull();
            orderCreatedDLTCounter.incrementAndGet();
        }

    }


    @BeforeEach
    void setUp(){
        kafkaTestListener.dispatchPreparingCounter.set(0);
        kafkaTestListener.orderDispatchCounter.set(0);
        kafkaTestListener.dispatchCompletedCounter.set(0);
        kafkaTestListener.orderCreatedDLTCounter.set(0);

        WiremockUtils.reset();

        /*kafkaListenerEndpointRegistry.getListenerContainers().forEach(container ->
            ContainerTestUtils.waitForAssignment(container, embeddedKafkaBroker.getPartitionsPerTopic())
        );*/

        // Wait until the partitions are assigned.  The application listener container has one topic and the test
        // listener container has multiple topics, so take that into account when awaiting for topic assignment.
        kafkaListenerEndpointRegistry.getListenerContainers().forEach(container ->
            ContainerTestUtils.waitForAssignment(container,
                Objects.requireNonNull(container.getContainerProperties().getTopics()).length * embeddedKafkaBroker.getPartitionsPerTopic()));
    }

    @Test
    void testOrderDispatchFlow() throws Exception{

        stubWiremock("/api/stock?item=my-item", 200, "true" );

        OrderCreated orderCreated = TestEventData.buildOrderCreatedEvent(randomUUID(), "my-item");
        sendMessage(ORDER_CREATED_TOPIC, randomUUID().toString(), orderCreated);

        await().atMost(3, TimeUnit.SECONDS).pollDelay(100, TimeUnit.MILLISECONDS)
            .until(kafkaTestListener.dispatchPreparingCounter::get, equalTo(1));
        await().atMost(1, TimeUnit.SECONDS).pollDelay(100, TimeUnit.MILLISECONDS)
            .   until(kafkaTestListener.orderDispatchCounter::get, equalTo(1));
        await().atMost(1, TimeUnit.SECONDS).pollDelay(100, TimeUnit.MILLISECONDS)
            .until(kafkaTestListener.dispatchCompletedCounter::get, equalTo(1));

        Assertions.assertThat(kafkaTestListener.orderCreatedDLTCounter.get()).isEqualTo(0);
        
    }

    @Test
    void testOrderDispatchFlow_NotRetryableException() throws Exception{
        // The call to the stock service is stubbed to return a 400 Bad Request which results in a not-retryable exception
        // being thrown, so the event is sent to the dead letter topic and the outbound events are never sent
        stubWiremock("/api/stock?item=my-item", 400, "Bad Request" );

        OrderCreated orderCreated = TestEventData.buildOrderCreatedEvent(randomUUID(), "my-item");
        sendMessage(ORDER_CREATED_TOPIC, randomUUID().toString(), orderCreated);

        await().atMost(3, TimeUnit.SECONDS).pollDelay(100, TimeUnit.MILLISECONDS)
            .until(kafkaTestListener.orderCreatedDLTCounter::get, equalTo(1));
        //        TimeUnit.SECONDS.sleep(3);
        Assertions.assertThat(kafkaTestListener.dispatchPreparingCounter.get()).isEqualTo(0);
        Assertions.assertThat(kafkaTestListener.orderDispatchCounter.get()).isEqualTo(0);
        Assertions.assertThat(kafkaTestListener.dispatchCompletedCounter.get()).isEqualTo(0);
    }

    @Test
    void testOrderDispatchFlow_RetryUntilSuccess() throws Exception{
         // The call to the stock service is stubbed to initially return a 503 Service Unavailable response, resulting in a
         // retryable exception being thrown.  On the subsequent attempt it is stubbed to then succeed, so the outbound events
         // are sent.

        stubWiremock("/api/stock?item=my-item",
            503,
            "Service Unavailable",
            "failOnce",
            Scenario.STARTED,
            "succeedNextTime");

        stubWiremock("/api/stock?item=my-item",
            200,
            "true",
            "failOnce",
            "succeedNextTime",
            "succeedNextTime");


        OrderCreated orderCreated = TestEventData.buildOrderCreatedEvent(randomUUID(), "my-item");
        sendMessage(ORDER_CREATED_TOPIC, randomUUID().toString(), orderCreated);

        await().atMost(3, TimeUnit.SECONDS).pollDelay(100, TimeUnit.MILLISECONDS)
            .until(kafkaTestListener.dispatchPreparingCounter::get, equalTo(1));
        await().atMost(1, TimeUnit.SECONDS).pollDelay(100, TimeUnit.MILLISECONDS)
            .until(kafkaTestListener.orderDispatchCounter::get, equalTo(1));
        await().atMost(1, TimeUnit.SECONDS).pollDelay(100, TimeUnit.MILLISECONDS)
            .until(kafkaTestListener.dispatchCompletedCounter::get, equalTo(1));
        await().atMost(1, TimeUnit.SECONDS).pollDelay(100, TimeUnit.MILLISECONDS)
            .until(kafkaTestListener.orderCreatedDLTCounter::get, equalTo(0));

        Assertions.assertThat(kafkaTestListener.orderCreatedDLTCounter.get()).isEqualTo(0);
    }

    @Test
    void testOrderDispatchFlow_RetryUntilFailure() throws Exception{

        stubWiremock("/api/stock?item=my-item", 503, "Service Unavailable");

        OrderCreated orderCreated = TestEventData.buildOrderCreatedEvent(randomUUID(), "my-item");
        sendMessage(ORDER_CREATED_TOPIC, randomUUID().toString(), orderCreated);

        await().atMost(5, TimeUnit.SECONDS).pollDelay(100, TimeUnit.MILLISECONDS)
            .until(kafkaTestListener.orderCreatedDLTCounter::get, equalTo(1));

        Assertions.assertThat(kafkaTestListener.dispatchPreparingCounter.get()).isEqualTo(0);
        Assertions.assertThat(kafkaTestListener.orderDispatchCounter.get()).isEqualTo(0);
        Assertions.assertThat(kafkaTestListener.dispatchCompletedCounter.get()).isEqualTo(0);
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
