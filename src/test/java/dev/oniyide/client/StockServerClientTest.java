package dev.oniyide.client;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestTemplate;

import dev.oniyide.exception.RetryableException;


@ExtendWith(MockitoExtension.class)
class StockServerClientTest
{
    @Mock
    private RestTemplate restTemplate;
    private StockServerClient stockServerClient;
    private static final String STOCK_SERVICE_ENDPOINT = "http://localhost:9001/api/stock";
    private static final String ITEM = "my-item";
    private static final String STOCK_SERVICE_QUERY = STOCK_SERVICE_ENDPOINT + "?item="+ITEM;

    @BeforeEach
    void setUp()
    {
        stockServerClient = new StockServerClient(restTemplate, STOCK_SERVICE_ENDPOINT);
    }

    @Test
    void checkAvailability()
    {
        when(restTemplate.getForEntity(STOCK_SERVICE_QUERY, String.class))
            .thenReturn(new ResponseEntity<>("item", HttpStatusCode.valueOf(200)));
        Assertions.assertThat(stockServerClient.checkAvailability(ITEM)).isTrue();
        verify(restTemplate).getForEntity(STOCK_SERVICE_QUERY, String.class);
    }

    @Test
    void checkAvailability_serverException()
    {
        doThrow(new HttpServerErrorException(HttpStatusCode.valueOf(500))).when(restTemplate).getForEntity(STOCK_SERVICE_QUERY, String.class);
        assertThrows(RetryableException.class, ()-> stockServerClient.checkAvailability(ITEM));
        verify(restTemplate).getForEntity(STOCK_SERVICE_QUERY, String.class);
    }

    @Test
    void checkAvailability_accessException()
    {
        doThrow(new ResourceAccessException("error accessing resource")).when(restTemplate).getForEntity(STOCK_SERVICE_QUERY, String.class);
        Exception exception = assertThrows(RetryableException.class, ()-> stockServerClient.checkAvailability(ITEM));
        Assertions.assertThat(exception.getMessage()).isEqualTo("org.springframework.web.client.ResourceAccessException: error accessing resource");
        verify(restTemplate).getForEntity(STOCK_SERVICE_QUERY, String.class);
    }

    @Test
    void checkAvailability_runtimeException()
    {
        doThrow(new RuntimeException("runtime exception")).when(restTemplate).getForEntity(STOCK_SERVICE_QUERY, String.class);
        Exception exception = assertThrows(Exception.class, ()-> stockServerClient.checkAvailability(ITEM));
        Assertions.assertThat(exception.getMessage()).isEqualTo("runtime exception");

        verify(restTemplate).getForEntity(STOCK_SERVICE_QUERY, String.class);
    }
}