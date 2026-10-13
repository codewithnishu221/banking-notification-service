package com.banking.notificationservice.service;

import java.math.BigDecimal;
import java.util.Map;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import lombok.extern.slf4j.Slf4j;

@Service 
@Slf4j 
public class NotificationService {


   @KafkaListener(
    topics = "transaction.otp.generated", 
    groupId = "notification-service-group")

    public void consumeOtpGenerated(@Payload  Map<String, Object> payload) {
        try {
            String accountNumber = (String)payload.get("senderAccountNumber");
            String otp = (String)payload.get("otp");
            String transactionId = (String) payload.get("transactionId");
         BigDecimal amount = new BigDecimal(payload.get("amount").toString());
         String reason = (String)payload.get("reason");

         
             String subject = "Transaction verification required";

            String message = String.format(
                    "Suspicious activity detected on your account. "
                    + "Reason: %s. "
                    + "Transaction %s for amount %s is pending verification. "
                    + "Your OTP is valid for 5 minutes. "
                    + "If this wasn't you, contact your bank immediately.",
                    reason,
                    transactionId,
                    amount
            );
           sendAlert(accountNumber, subject, message);
           log.info(
                    "OTP notification prepared for transaction {}",
                    transactionId
            );
        } catch (Exception e) {
            log.error("Error sending OTP notification: {}", e.getMessage());
        }
    }

    private void sendAlert(String accountNumber, String subject, String message){
        log.info("Notification delivery is not implemented yet. "
                + "Account reference: {}", maskAccountNumber(accountNumber));
    }

    private String maskAccountNumber(String accountNumber){
        if(accountNumber == null || accountNumber.length() <= 14){
            return "*****";
        }
        return "****" + accountNumber.substring(accountNumber.length()-4);
    }
}
