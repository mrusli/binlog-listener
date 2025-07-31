package com.pyramix;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.shyiko.mysql.binlog.event.UpdateRowsEventData;
import com.github.shyiko.mysql.binlog.event.WriteRowsEventData;

public class MyBinLogListener {

	private static final Logger log = LoggerFactory.getLogger(MyBinLogListener.class);
	
	public static void main(String[] args) throws IllegalStateException, IOException {
		log.info("HelloWorld!!!!");
		BinaryLogClient client = new BinaryLogClient(
					"localhost",
					3304,
					"e021_swi_core",
					"root",
					"password"
				);
		client.setServerId(2);
		client.registerEventListener(new BinaryLogClient.EventListener() {
			
			@Override
			public void onEvent(Event event) {
				log.info("Received event type: {}", event.getHeader().getEventType());
				
				EventType eventType = event.getHeader().getEventType();
				if (eventType == EventType.TABLE_MAP) {
					TableMapEventData tableMapEventData = event.getData();
					// long tableId = tableMapEventData.getTableId();
					String databaseName = tableMapEventData.getDatabase();
					String tableName = tableMapEventData.getTable();
					log.info("database={}, table={}",databaseName,tableName);
				}
				if (eventType == EventType.EXT_UPDATE_ROWS) {
					UpdateRowsEventData updateRowsEventData = event.getData();
					log.info("update_row_id={}", (Object[])updateRowsEventData.getRows().getFirst().getValue());
				}
				if (eventType == EventType.EXT_WRITE_ROWS) {
					WriteRowsEventData writeRowsEventData = event.getData();
					log.info("new_row_id={}", (Object[])writeRowsEventData.getRows().getFirst());
				}
				
				
			}
		});
		client.connect();
		log.info(String.format("%s", client.isConnected()));		
	}

}
