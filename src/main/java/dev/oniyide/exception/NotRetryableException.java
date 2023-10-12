package dev.oniyide.exception;

public class NotRetryableException extends RuntimeException
{
    public NotRetryableException(Exception exception) {
        super(exception);
    }
}
