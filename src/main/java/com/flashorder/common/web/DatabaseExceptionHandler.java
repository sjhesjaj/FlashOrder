package com.flashorder.common.web;

import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLTransientConnectionException;
import org.springframework.dao.PessimisticLockingFailureException;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.CannotGetJdbcConnectionException;
import org.springframework.transaction.CannotCreateTransactionException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice
public class DatabaseExceptionHandler {

    @ExceptionHandler({PessimisticLockingFailureException.class, CannotGetJdbcConnectionException.class})
    public ResponseEntity<RetryResponse> unavailable() {
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                .header(HttpHeaders.RETRY_AFTER, "1")
                .body(new RetryResponse("RETRYABLE", "TEMPORARY_DATABASE_ERROR"));
    }

    @ExceptionHandler(CannotCreateTransactionException.class)
    public ResponseEntity<RetryResponse> cannotBegin(CannotCreateTransactionException failure) {
        // Spring wraps connection acquisition failures when starting a transaction.
        for (Throwable cause = failure.getCause(); cause != null; cause = cause.getCause()) {
            if (cause instanceof CannotGetJdbcConnectionException
                    || cause instanceof SQLTransientConnectionException
                    || cause instanceof SQLNonTransientConnectionException
                    || (cause instanceof SQLException sql && sql.getSQLState() != null
                            && sql.getSQLState().startsWith("08"))) {
                return unavailable();
            }
        }
        throw failure;
    }

    public record RetryResponse(String status, String reason) {
    }
}
