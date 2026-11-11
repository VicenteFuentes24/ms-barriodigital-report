package cl.barriodigital.ms_report.exception;

import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.transaction.CannotCreateTransactionException;
import org.springframework.http.*;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;

@RestControllerAdvice
public class ApiExceptionHandler {
    @ExceptionHandler(InvalidRangeException.class)
    public ProblemDetail invalidRange(InvalidRangeException ex) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST, ex.getMessage());
    }
    @ExceptionHandler(MethodArgumentTypeMismatchException.class)
    public ProblemDetail invalidType(MethodArgumentTypeMismatchException ex) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST, "Invalid query parameter: " + ex.getName());
    }
    @ExceptionHandler({DataAccessException.class, CannotCreateTransactionException.class})
    public ProblemDetail unavailable(RuntimeException ex) {
        LoggerFactory.getLogger(getClass()).atError().addKeyValue("failureType", ex.getClass().getSimpleName())
                .log("report_database_unavailable");
        return ProblemDetail.forStatusAndDetail(HttpStatus.SERVICE_UNAVAILABLE, "Reporting storage is temporarily unavailable");
    }
}
