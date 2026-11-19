package com.flashorder.order;

import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Optional;
import org.springframework.context.annotation.Profile;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

@RestController
@Profile("api")
public class OrderController {

    private final OrderMapper orders;

    public OrderController(OrderMapper orders) {
        this.orders = orders;
    }

    @GetMapping("/orders/{orderUid}")
    public ResponseEntity<OrderResponse> findOrder(
            @PathVariable String orderUid, @RequestHeader("X-User-Id") long userId) {
        if (userId <= 0) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Invalid user ID");
        }
        return ResponseEntity.of(Optional.ofNullable(orders.findOwnedOrder(userId, orderUid))
                .map(order -> new OrderResponse(
                        order.orderUid(), order.status(), order.expireAt().toInstant(ZoneOffset.UTC))));
    }

    public record OrderResponse(String orderUid, String status, Instant expireAt) {
    }
}
