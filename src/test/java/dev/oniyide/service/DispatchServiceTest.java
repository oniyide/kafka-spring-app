package dev.oniyide.service;

import static java.util.UUID.randomUUID;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.concurrent.CompletableFuture;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;

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

    @BeforeEach
    void
    setUp()
    {
        dispatchService = new DispatchService(kafkaProducer);
    }

    @Test
    void process() throws Exception
    {
        String key = randomUUID().toString();
        OrderCreated event = OrderCreated.builder().orderId(randomUUID()).item(randomUUID().toString()).build();

        when(kafkaProducer.send(anyString(), anyString(), any(OrderDispatched.class))).thenReturn(mock(CompletableFuture.class));
        when(kafkaProducer.send(anyString(), anyString(), any(DispatchPreparing.class))).thenReturn(mock(CompletableFuture.class));
        when(kafkaProducer.send(anyString(), anyString(), any(DispatchCompleted.class))).thenReturn(mock(CompletableFuture.class));

        dispatchService.process(key, event);

        verify(kafkaProducer).send(eq("dispatch.tracking"), eq(key), any(DispatchPreparing.class));
        verify(kafkaProducer).send(eq("order.dispatched"), eq(key), any(OrderDispatched.class));
        verify(kafkaProducer).send(eq("dispatch.tracking"), eq(key), any(DispatchCompleted.class));
    }

    @Test
    void process_dispatchTrackingProducerThrowsException()
    {
        String key = randomUUID().toString();
        OrderCreated event = OrderCreated.builder().orderId(randomUUID()).item(randomUUID().toString()).build();
        doThrow(new RuntimeException("dispatch tracking producer failure")).when(kafkaProducer)
            .send(eq("dispatch.tracking"), eq(key), any(DispatchPreparing.class));

        Exception exception = assertThrows(RuntimeException.class, () -> dispatchService.process(key, event));

        verify(kafkaProducer).send(eq("dispatch.tracking"), eq(key), any(DispatchPreparing.class));
        Assertions.assertThat(exception.getMessage()).isEqualTo("dispatch tracking producer failure");
        verifyNoMoreInteractions(kafkaProducer);
    }

    @Test
    void process_orderDispatchedProducerThrowsException()
    {
        String key = randomUUID().toString();
        OrderCreated event = OrderCreated.builder().orderId(randomUUID()).item(randomUUID().toString()).build();
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
        OrderCreated event = OrderCreated.builder().orderId(randomUUID()).item(randomUUID().toString()).build();
        when(kafkaProducer.send(anyString(), anyString(), any(DispatchPreparing.class))).thenReturn(mock(CompletableFuture.class));
        doThrow(new RuntimeException("order dispatched producer failure")).when(kafkaProducer)
            .send(eq("order.dispatched"), eq(key), any(OrderDispatched.class));

        Exception exception = assertThrows(RuntimeException.class, () -> dispatchService.process(key, event));

        verify(kafkaProducer).send(eq("dispatch.tracking"), eq(key), any(DispatchPreparing.class));
        verify(kafkaProducer).send(eq("order.dispatched"), eq(key), any(OrderDispatched.class));
        Assertions.assertThat(exception.getMessage()).isEqualTo("order dispatched producer failure");
        verifyNoMoreInteractions(kafkaProducer);
    }
}