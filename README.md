# Supply Chain Predictor - 3-Tier Microservices Architecture

A comprehensive 3-tier microservices system for predicting and analyzing supply chain disruptions using real-time data, machine learning, and event-driven architecture.

## Architecture Overview

### 3-Tier Design

**Data Layer:**
- PostgreSQL for structured data storage
- Redis for caching and real-time data
- RabbitMQ for event-driven messaging

**Processing Layer (Microservices):**
- API Gateway: Central entry point and routing
- Data Collection Service: Collects news, weather, and economic data
- ML Inference Service: AI analysis and predictions
- Risk Assessment Service: Business logic for risk calculations
- Notification Service: Real-time alerts and notifications

**Presentation Layer:**
- React.js Dashboard with Material-UI
- RESTful API integration
- Real-time updates and interactive charts

## Technology Stack

**Backend Services:**
- Python 3.11 with FastAPI
- PostgreSQL 15
- Redis 7
- RabbitMQ 3
- Docker & Docker Compose

**Frontend:**
- React 18 with TypeScript
- Material-UI (MUI)
- React Query for state management
- Recharts for data visualization

**Machine Learning:**
- scikit-learn
- pandas & numpy
- Custom ML models for risk prediction

## Quick Start

### Prerequisites
- Docker and Docker Compose
- Git

### Installation

1. **Clone the repository:**
```bash
git clone <repository-url>
cd supply-chain-predictor
```

2. **Configure environment:**
```bash
cp .env.example .env
# Edit .env with your configuration
```

3. **Start all services:**
```bash
docker-compose up -d
```

4. **Access the application:**
- Frontend: http://localhost:3000
- API Gateway: http://localhost:8000
- RabbitMQ Management: http://localhost:15672 (admin/admin)

### Service Endpoints

- **API Gateway**: http://localhost:8000
- **Data Collector**: http://localhost:8001
- **ML Inference**: http://localhost:8002
- **Risk Assessment**: http://localhost:8003
- **Notification Service**: http://localhost:8004
- **Frontend**: http://localhost:3000

- **News**: Global news feeds for supply chain events
- **Weather**: Severe weather patterns affecting logistics
- **Economic**: Trade data, port congestion, fuel prices
- **Geopolitical**: Political events affecting trade routes

## License

MIT License
