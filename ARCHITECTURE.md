# 3-Tier Microservices Architecture

## Overview
This supply chain predictor application is built using a 3-tier microservices architecture with event-driven communication patterns.

## Architecture Layers

### 1. Data Layer
- **PostgreSQL**: Primary database for structured data storage
- **Redis**: Caching layer and real-time data storage
- **Message Queue**: RabbitMQ for event-driven communication

### 2. Processing Layer (Microservices)
- **API Gateway**: Central entry point and routing
- **Data Collection Service**: Collects news, weather, and economic data
- **ML Inference Service**: AI analysis and predictions
- **Risk Assessment Service**: Business logic for risk calculations
- **Notification Service**: Real-time alerts and notifications

### 3. Presentation Layer
- **React.js Dashboard**: Modern web interface
- **RESTful APIs**: Communication between frontend and services

## Event-Driven Architecture
- Asynchronous processing using RabbitMQ
- Event publishing for data updates
- Real-time alert generation
- Scalable message handling

## Technology Stack
- **Backend**: Python (FastAPI), PostgreSQL, Redis, RabbitMQ
- **Frontend**: React.js, TypeScript, Material-UI
- **Infrastructure**: Docker, Docker Compose
- **Monitoring**: Health checks and logging
