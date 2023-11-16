package com.coralocean.exception;

public class ExportException extends RuntimeException{

    private final String errorMessage;
    public ExportException(String errorMessage, Throwable cause) {
        super(errorMessage, cause);
        this.errorMessage = errorMessage;
    }
    public ExportException(String errorMessage) {
        super(errorMessage);
        this.errorMessage = errorMessage;
    }

    public ExportException(Throwable cause) {
        super(cause);
        this.errorMessage = cause.getLocalizedMessage();
    }

    public String getErrorMessage() {
        return errorMessage;
    }
}
