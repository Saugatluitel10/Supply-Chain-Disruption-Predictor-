"""
Shared message queue utilities for event-driven architecture
"""

import pika
import json
import logging
from typing import Dict, Any, Callable
import os
from datetime import datetime

logger = logging.getLogger(__name__)

class MessageQueue:
    def __init__(self):
        self.connection = None
        self.channel = None
        self.rabbitmq_url = os.getenv("RABBITMQ_URL", "amqp://admin:admin@localhost:5672/")
        self.connect()
    
    def connect(self):
        """Establish connection to RabbitMQ"""
        try:
            parameters = pika.URLParameters(self.rabbitmq_url)
            self.connection = pika.BlockingConnection(parameters)
            self.channel = self.connection.channel()
            
            # Declare exchanges
            self.channel.exchange_declare(exchange='supply_chain_events', exchange_type='topic', durable=True)
            self.channel.exchange_declare(exchange='risk_alerts', exchange_type='topic', durable=True)
            self.channel.exchange_declare(exchange='ml_predictions', exchange_type='topic', durable=True)
            
            logger.info("Connected to RabbitMQ successfully")
        except Exception as e:
            logger.error(f"Failed to connect to RabbitMQ: {e}")
            raise
    
    def publish_event(self, exchange: str, routing_key: str, message: Dict[Any, Any]):
        """Publish an event to the message queue"""
        try:
            if not self.connection or self.connection.is_closed:
                self.connect()
            
            # Add timestamp to message
            message['timestamp'] = datetime.utcnow().isoformat()
            
            self.channel.basic_publish(
                exchange=exchange,
                routing_key=routing_key,
                body=json.dumps(message, default=str),
                properties=pika.BasicProperties(
                    delivery_mode=2,  # Make message persistent
                    content_type='application/json'
                )
            )
            logger.info(f"Published event to {exchange}.{routing_key}")
        except Exception as e:
            logger.error(f"Failed to publish event: {e}")
            raise
    
    def consume_events(self, queue_name: str, callback: Callable, exchange: str, routing_key: str):
        """Consume events from a queue"""
        try:
            if not self.connection or self.connection.is_closed:
                self.connect()
            
            # Declare queue and bind to exchange
            self.channel.queue_declare(queue=queue_name, durable=True)
            self.channel.queue_bind(exchange=exchange, queue=queue_name, routing_key=routing_key)
            
            def wrapper(ch, method, properties, body):
                try:
                    message = json.loads(body)
                    callback(message)
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                except Exception as e:
                    logger.error(f"Error processing message: {e}")
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            
            self.channel.basic_qos(prefetch_count=1)
            self.channel.basic_consume(queue=queue_name, on_message_callback=wrapper)
            
            logger.info(f"Starting to consume from {queue_name}")
            self.channel.start_consuming()
            
        except Exception as e:
            logger.error(f"Failed to consume events: {e}")
            raise
    
    def close(self):
        """Close the connection"""
        if self.connection and not self.connection.is_closed:
            self.connection.close()

# Event types and routing keys
class EventTypes:
    DATA_COLLECTED = "data.collected"
    RISK_CALCULATED = "risk.calculated"
    ALERT_GENERATED = "alert.generated"
    ML_PREDICTION = "ml.prediction"
    BUSINESS_IMPACT = "business.impact"

# Exchanges
class Exchanges:
    SUPPLY_CHAIN_EVENTS = "supply_chain_events"
    RISK_ALERTS = "risk_alerts"
    ML_PREDICTIONS = "ml_predictions"
