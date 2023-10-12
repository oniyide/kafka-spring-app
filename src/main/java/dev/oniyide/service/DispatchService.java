package dev.oniyide.service;

import java.time.LocalTime;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import dev.oniyide.client.StockServerClient;
import dev.oniyide.message.DispatchCompleted;
import dev.oniyide.message.DispatchPreparing;
import dev.oniyide.message.OrderCreated;
import dev.oniyide.message.OrderDispatched;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;


@Slf4j
@Service
@RequiredArgsConstructor
public class DispatchService
{
    private static final String ORDER_DISPATCHED_TOPIC ="order.dispatched";
    private static final String DISPATCH_TRACKING_TOPIC ="dispatch.tracking";
    private static final UUID APPLICATION_ID = UUID.randomUUID();
    private final KafkaTemplate<String, Object> kafkaProducer;
    private final StockServerClient stockServerClient;

    public void process(String key, OrderCreated orderCreated) throws ExecutionException, InterruptedException{
        Boolean available = stockServerClient.checkAvailability(orderCreated.getItem());
        if(available)
        {
            UUID orderId = orderCreated.getOrderId();

            DispatchPreparing dispatchPreparing = DispatchPreparing.builder()
                .orderId(orderId)
                .build();
            kafkaProducer.send(DISPATCH_TRACKING_TOPIC, key, dispatchPreparing).get();

            OrderDispatched orderDispatched = OrderDispatched.builder()
                .orderId(orderId)
                .processedById(APPLICATION_ID)
                .notes("Dispatched: " + orderCreated.getItem())
                .build();
            kafkaProducer.send(ORDER_DISPATCHED_TOPIC, key, orderDispatched).get();

            DispatchCompleted dispatchCompleted = DispatchCompleted.builder()
                .orderId(orderId)
                .date(LocalTime.now().toString())
                .build();
            kafkaProducer.send(DISPATCH_TRACKING_TOPIC, key, dispatchCompleted).get();

            log.info("Sent messages: key : {} orderCreated.getOrderId : {} - processedById: {}", key, orderId, APPLICATION_ID);
        }
        else {
            log.info("Item {} is unavailable", orderCreated.getItem());
        }
    }
}
