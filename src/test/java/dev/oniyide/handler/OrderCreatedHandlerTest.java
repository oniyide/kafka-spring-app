package dev.oniyide.handler;

import static java.util.UUID.randomUUID;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import dev.oniyide.exception.NotRetryableException;
import dev.oniyide.exception.RetryableException;
import dev.oniyide.message.OrderCreated;
import dev.oniyide.service.DispatchService;


@ExtendWith(MockitoExtension.class)
class OrderCreatedHandlerTest
{
    private OrderCreatedHandler handler;
    @Mock
    private DispatchService dispatchServiceMock;

    @BeforeEach
    void setUp()
    {
        handler = new OrderCreatedHandler(dispatchServiceMock);
    }

    @Test
    void listen() throws Exception
    {
        String key = randomUUID().toString();
        OrderCreated testEvent = OrderCreated.builder().orderId(randomUUID()).item(randomUUID().toString()).build();
        handler.listen(0, key, testEvent);
        verify(dispatchServiceMock).process(key, testEvent);

    }

    @Test
    void listen_throwsNotRetryableException() throws Exception
    {
        String key = randomUUID().toString();
        OrderCreated testEvent = OrderCreated.builder().orderId(randomUUID()).item(randomUUID().toString()).build();
        doThrow(new RuntimeException("service failure")).when(dispatchServiceMock).process(key, testEvent);

        Exception exception = assertThrows(NotRetryableException.class, () -> handler.listen(0, key,testEvent));

        Assertions.assertThat(exception.getMessage()).isEqualTo("java.lang.RuntimeException: service failure");
        verify(dispatchServiceMock).process(key, testEvent);
    }

    @Test
    void listen_throwsRetryableException() throws Exception
    {
        String key = randomUUID().toString();
        OrderCreated testEvent = OrderCreated.builder().orderId(randomUUID()).item(randomUUID().toString()).build();
        doThrow(new RetryableException("service failure")).when(dispatchServiceMock).process(key, testEvent);

        Exception exception = assertThrows(RetryableException.class, () -> handler.listen(0, key,testEvent));

        Assertions.assertThat(exception.getMessage()).isEqualTo("service failure");
        verify(dispatchServiceMock).process(key, testEvent);
    }
}