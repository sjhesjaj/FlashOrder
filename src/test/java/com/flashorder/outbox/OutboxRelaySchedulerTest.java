package com.flashorder.outbox;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.flashorder.outbox.OutboxRelay.BatchResult;
import org.junit.jupiter.api.Test;

class OutboxRelaySchedulerTest {
    private static final int BATCH_SIZE = 3;
    private static final long MAX_ROUND_MS = 60_000;

    private final OutboxRelay relay = mock(OutboxRelay.class);
    private final OutboxRelayScheduler scheduler =
            new OutboxRelayScheduler(relay, mock(OutboxRelayStore.class), BATCH_SIZE, MAX_ROUND_MS);

    @Test
    void drainsFullBatchesInTheSameRoundAndStopsAfterAShortBatch() {
        BatchResult full = new BatchResult(BATCH_SIZE, BATCH_SIZE);
        when(relay.publishBatch())
                .thenReturn(full, full, new BatchResult(2, 2))
                .thenThrow(new AssertionError("The short batch must end the round"));

        scheduler.publishDueMessages();

        verify(relay, times(3)).publishBatch();
    }

    @Test
    void stopsWhenSomeClaimedMessagesWereNotMarkedPublished() {
        when(relay.publishBatch())
                .thenReturn(new BatchResult(BATCH_SIZE, BATCH_SIZE - 1))
                .thenThrow(new AssertionError("The incomplete batch must end the round"));

        scheduler.publishDueMessages();

        verify(relay).publishBatch();
    }
}
