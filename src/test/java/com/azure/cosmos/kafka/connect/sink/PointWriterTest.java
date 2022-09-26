// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.kafka.connect.sink;

import com.azure.cosmos.CosmosContainer;
import com.azure.cosmos.CosmosException;
import com.azure.cosmos.implementation.BadRequestException;
import com.azure.cosmos.implementation.HttpConstants;
import com.azure.cosmos.implementation.RequestTimeoutException;
import com.azure.cosmos.models.CosmosItemResponse;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static junit.framework.Assert.assertTrue;
import static junit.framework.TestCase.assertEquals;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class PointWriterTest {
    private final int MAX_RETRY_COUNT = 2;
    private final String TOPIC_NAME = "testtopic";

    private CosmosContainer container;
    private PointWriter pointWriter;

    @Before
    public void setup(){
        container = Mockito.mock(CosmosContainer.class);
        pointWriter = new PointWriter(container, MAX_RETRY_COUNT);
    }

    @Test
    public void testPointWriteSuccess() {
        SinkRecord record1 = createSinkRecord();
        SinkRecord record2 = createSinkRecord();
        CosmosItemResponse<Object> itemResponse = Mockito.mock(CosmosItemResponse.class);

        Mockito.when(container.upsertItem(record1.value())).thenReturn(itemResponse);
        Mockito.when(container.upsertItem(record2.value())).thenReturn(itemResponse);

        SinkWriteResponse response = pointWriter.write(Arrays.asList(record1, record2));
        assertEquals(2, response.getSucceededRecords().size());
        assertEquals(record1, response.getSucceededRecords().get(0));
        assertEquals(record2, response.getSucceededRecords().get(1));
        assertEquals(0, response.getFailedRecordResponses().size());
    }

    @Test
    public void testPointWriteWithNonTransientException() {
        SinkRecord record1 = createSinkRecord();
        SinkRecord record2 = createSinkRecord();

        CosmosItemResponse<Object> itemResponse = Mockito.mock(CosmosItemResponse.class);
        Mockito.when(container.upsertItem(record1.value())).thenReturn(itemResponse);
        Mockito.when(container.upsertItem(record2.value())).thenThrow(new BadRequestException("Test"));

        SinkWriteResponse response = pointWriter.write(Arrays.asList(record1, record2));
        // Validate record 1 succeeded
        assertEquals(1, response.getSucceededRecords().size());
        assertEquals(record1, response.getSucceededRecords().get(0));
        assertEquals(1, response.getFailedRecordResponses().size());
        verify(container, times(1)).upsertItem(record1.value());

        // Validate record2 failed
        assertEquals(record2, response.getFailedRecordResponses().get(0).getSinkRecord());
        assertTrue(response.getFailedRecordResponses().get(0).getException() instanceof CosmosException);
        assertEquals(HttpConstants.StatusCodes.BADREQUEST, ((CosmosException)response.getFailedRecordResponses().get(0).getException()).getStatusCode());
        verify(container, times(1)).upsertItem(record2.value());
    }

    @Test
    public void testPointWriteWithTransientException() {
        SinkRecord record1 = createSinkRecord();
        SinkRecord record2 = createSinkRecord();

        CosmosItemResponse<Object> itemResponse = Mockito.mock(CosmosItemResponse.class);
        Mockito.when(container.upsertItem(record1.value())).thenReturn(itemResponse);
        Mockito
                .when(container.upsertItem(record2.value()))
                .thenThrow(new RequestTimeoutException())
                .thenThrow(new RequestTimeoutException())
                .thenReturn(itemResponse);

        SinkWriteResponse response = pointWriter.write(Arrays.asList(record1, record2));

        assertEquals(2, response.getSucceededRecords().size());
        assertEquals(record1, response.getSucceededRecords().get(0));
        assertEquals(record2, response.getSucceededRecords().get(1));
        assertEquals(0, response.getFailedRecordResponses().size());

        verify(container, times(1)).upsertItem(record1.value());
        verify(container, times(3)).upsertItem(record2.value());
    }

    private SinkRecord createSinkRecord() {
        Schema stringSchema = new ConnectSchema(Schema.Type.STRING);
        Schema mapSchema = new ConnectSchema(Schema.Type.MAP);
        Map<String, String> map = new HashMap<>();
        map.put("foo", "baaarrrrrgh");
        map.put("id", UUID.randomUUID().toString());

        return new SinkRecord(TOPIC_NAME, 1, stringSchema, "nokey", mapSchema, map, 0L);
    }
}
