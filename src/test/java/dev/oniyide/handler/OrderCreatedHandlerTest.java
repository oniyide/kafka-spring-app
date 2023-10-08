package dev.oniyide.handler;

import static java.util.UUID.randomUUID;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

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
    void listen_throwsException() throws Exception
    {
        String key = randomUUID().toString();
        OrderCreated testEvent = OrderCreated.builder().orderId(randomUUID()).item(randomUUID().toString()).build();
        doThrow(new RuntimeException("service failure")).when(dispatchServiceMock).process(key, testEvent);

        handler.listen(0, key,testEvent);
        verify(dispatchServiceMock).process(key, testEvent);

    }
}