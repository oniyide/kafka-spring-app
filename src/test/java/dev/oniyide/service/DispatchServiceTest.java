package dev.oniyide.service;

import static java.util.UUID.randomUUID;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;

import dev.oniyide.client.StockServerClient;
import dev.oniyide.message.DispatchCompleted;
import dev.oniyide.message.DispatchPreparing;
import dev.oniyide.message.OrderCreated;
import dev.oniyide.message.OrderDispatched;


@ExtendWith(MockitoExtension.class)
class DispatchServiceTest
{
    private DispatchService dispatchService;

    @Mock
    private KafkaTemplate<String, Object> kafkaProducer;
    @Mock
    private StockServerClient stockServerClient;

    @BeforeEach
    void
    setUp()
    {
        dispatchService = new DispatchService(kafkaProducer, stockServerClient);
    }

    @Test
    void process() throws Exception
    {
        String key = randomUUID().toString();
        OrderCreated event = OrderCreated.builder().orderId(randomUUID()).item("my-item").build();

        when(kafkaProducer.send(anyString(), anyString(), any(OrderDispatched.class))).thenReturn(mock(CompletableFuture.class));
        when(kafkaProducer.send(anyString(), anyString(), any(DispatchPreparing.class))).thenReturn(mock(CompletableFuture.class));
        when(kafkaProducer.send(anyString(), anyString(), any(DispatchCompleted.class))).thenReturn(mock(CompletableFuture.class));

        when(stockServerClient.checkAvailability(anyString())).thenReturn(true);

        dispatchService.process(key, event);

        verify(kafkaProducer).send(eq("dispatch.tracking"), eq(key), any(DispatchPreparing.class));
        verify(kafkaProducer).send(eq("order.dispatched"), eq(key), any(OrderDispatched.class));
        verify(kafkaProducer).send(eq("dispatch.tracking"), eq(key), any(DispatchCompleted.class));
        verify(stockServerClient).checkAvailability(eq("my-item"));
    }

    @Test
    void process_dispatchTrackingProducerThrowsException()
    {
        String key = randomUUID().toString();
        OrderCreated event = OrderCreated.builder().orderId(randomUUID()).item("my-item").build();
        when(stockServerClient.checkAvailability(anyString())).thenReturn(true);

        doThrow(new RuntimeException("dispatch tracking producer failure")).when(kafkaProducer)
            .send(eq("dispatch.tracking"), eq(key), any(DispatchPreparing.class));

        Exception exception = assertThrows(RuntimeException.class, () -> dispatchService.process(key, event));

        verify(kafkaProducer).send(eq("dispatch.tracking"), eq(key), any(DispatchPreparing.class));
        Assertions.assertThat(exception.getMessage()).isEqualTo("dispatch tracking producer failure");
        verify(stockServerClient).checkAvailability(eq("my-item"));
        verifyNoMoreInteractions(kafkaProducer);


    }

    @Test
    void process_orderDispatchedProducerThrowsException()
    {
        String key = randomUUID().toString();
        OrderCreated event = OrderCreated.builder().orderId(randomUUID()).item("my-item").build();

        when(stockServerClient.checkAvailability(anyString())).thenReturn(true);
        when(kafkaProducer.send(anyString(), anyString(), any(DispatchPreparing.class))).thenReturn(mock(CompletableFuture.class));
        doThrow(new RuntimeException("order dispatched producer failure")).when(kafkaProducer)
            .send(eq("order.dispatched"), eq(key), any(OrderDispatched.class));

        Exception exception = assertThrows(RuntimeException.class, () -> dispatchService.process(key, event));

        verify(kafkaProducer).send(eq("dispatch.tracking"), eq(key), any(DispatchPreparing.class));
        verify(kafkaProducer).send(eq("order.dispatched"), eq(key), any(OrderDispatched.class));
        Assertions.assertThat(exception.getMessage()).isEqualTo("order dispatched producer failure");
        verifyNoMoreInteractions(kafkaProducer);
    }


    @Test
    void process_dispatchCompletedProducerThrowsException()
    {
        String key = randomUUID().toString();
        OrderCreated event = OrderCreated.builder().orderId(randomUUID()).item("my-item").build();

        when(stockServerClient.checkAvailability(anyString())).thenReturn(true);
        when(kafkaProducer.send(anyString(), anyString(), any(DispatchPreparing.class))).thenReturn(mock(CompletableFuture.class));
        when(kafkaProducer.send(anyString(), anyString(), any(OrderDispatched.class))).thenReturn(mock(CompletableFuture.class));
        doThrow(new RuntimeException("dispatch completed producer failure")).when(kafkaProducer)
            .send(eq("dispatch.tracking"), eq(key), any(DispatchCompleted.class));

        Exception exception = assertThrows(RuntimeException.class, () -> dispatchService.process(key, event));

        verify(stockServerClient).checkAvailability(eq("my-item"));
        verify(kafkaProducer).send(eq("dispatch.tracking"), eq(key), any(DispatchPreparing.class));
        verify(kafkaProducer).send(eq("order.dispatched"), eq(key), any(OrderDispatched.class));
        verify(kafkaProducer).send(eq("dispatch.tracking"), eq(key), any(DispatchCompleted.class));

        Assertions.assertThat(exception.getMessage()).isEqualTo("dispatch completed producer failure");

        verifyNoMoreInteractions(kafkaProducer);
    }

    @Test
    void process_itemNotAvailable() throws ExecutionException, InterruptedException
    {
        String key = randomUUID().toString();
        OrderCreated event = OrderCreated.builder().orderId(randomUUID()).item("my-item").build();

        when(stockServerClient.checkAvailability(anyString())).thenReturn(false);
        dispatchService.process(key, event);

        verify(stockServerClient).checkAvailability(eq("my-item"));
        verifyNoInteractions(kafkaProducer);
    }
}