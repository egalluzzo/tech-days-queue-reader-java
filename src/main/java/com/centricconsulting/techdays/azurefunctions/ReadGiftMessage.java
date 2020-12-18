package com.centricconsulting.techdays.azurefunctions;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.azure.functions.annotation.FunctionName;
import com.microsoft.azure.functions.annotation.ServiceBusQueueTrigger;

public class ReadGiftMessage {

    // No getters and setters because I'm lazy and this is just a DTO, in a toy project.
    private static class GiftMessage {
        public UUID messageId;
        public Volume giftBoundingBox;
        public String recipient;
    }

    private static class Volume {
        public double length;
        public double height;
        public double width;
    }

    @FunctionName("ReadGiftMessage")
    public void readGiftMessage(
            @ServiceBusQueueTrigger(
                    name = "msg",
                    queueName = "eric-galluzzo",
                    connection = "TECHDAYSPRODUCTIONLINES_SERVICEBUS")
            final String message,
            final ExecutionContext context) {

        context.getLogger().info("Read message: " + message);

        final ObjectMapper mapper = new ObjectMapper();
        final GiftMessage giftMessage;
        try {
            giftMessage = mapper.readValue(message, GiftMessage.class);
        } catch (Exception e) {
            context.getLogger().log(Level.SEVERE, "Could not parse service bus message", e);
            throw new RuntimeException("Could not parse service bus message", e);
        }

        writeGiftMessageToDatabase(giftMessage, context.getLogger());
    }

    private void writeGiftMessageToDatabase(final GiftMessage giftMessage,
                                            final Logger logger) {
        try {
            Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Could not load SQL Server JDBC driver", e);
            throw new RuntimeException("Could not load SQL Server JDBC driver", e);
        }

        String connectionUrl = System.getenv("GIFTS_DB_CONFIG");
        try (final Connection conn = DriverManager.getConnection(connectionUrl)) {
            String sql = "INSERT INTO gifts " +
                    "(id, length, width, height, production_line, recipient, creation_date) " +
                    "VALUES (?, ?, ?, ?, 'eric-galluzzo', ?, CURRENT_TIMESTAMP)";
            try (final PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setString(1, giftMessage.messageId.toString());
                ps.setDouble(2, giftMessage.giftBoundingBox.length);
                ps.setDouble(3, giftMessage.giftBoundingBox.width);
                ps.setDouble(4, giftMessage.giftBoundingBox.height);
                ps.setString(5, giftMessage.recipient);
                ps.execute();

                logger.log(Level.INFO, "Wrote gift message to database");
            }
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Exception while writing to database", e);
            throw new RuntimeException("Exception when writing to database", e);
        }
    }
}
