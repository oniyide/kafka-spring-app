package dev.oniyide.client;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestTemplate;

import dev.oniyide.exception.RetryableException;
import lombok.extern.slf4j.Slf4j;


@Slf4j
@Component
public class StockServerClient
{
    private final RestTemplate restTemplate;
    private final String stockServiceEndpoint;

    public StockServerClient(final RestTemplate restTemplate, @Value("${dispatch.stockServiceEndpoint}") final String stockServiceEndpoint)
    {
        this.restTemplate = restTemplate;
        this.stockServiceEndpoint = stockServiceEndpoint;
    }

    public Boolean checkAvailability(String item){
        try
        {
            ResponseEntity<String> response = restTemplate.getForEntity(stockServiceEndpoint+"?item="+item, String.class);
            if(response.getStatusCode().value() != 200){
                throw new RuntimeException("error "+ response.getStatusCode());
            }
            return true;
        }
        catch (HttpServerErrorException | ResourceAccessException e) {
            log.warn("Failure calling external service", e);
            throw new RetryableException(e);
        }
        catch (Exception e){
            log.error("Exception thrown: {} {}", e.getClass().getName(), e);
            throw e;
        }
    }
}
