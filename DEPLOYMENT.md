# Deployment Guide - Supply Chain Predictor

## Features

### Real-time Data Collection
- Automated collection from multiple sources
- News, weather, and economic data integration
- Event-driven processing pipeline

### AI-Powered Analysis
- Machine learning models for risk prediction
- Impact severity assessment
- Disruption duration forecasting
- Business-specific impact analysis

### Event-Driven Architecture
- Asynchronous message processing
- Real-time alert generation
- Scalable microservices communication

### Interactive Dashboard
- Modern React.js interface
- Real-time data visualization
- Business profile management
- Alert management system

## Development

### Running Individual Services

**API Gateway:**
```bash
cd services/api-gateway
pip install -r requirements.txt
uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```

**Data Collector:**
```bash
cd services/data-collector
pip install -r requirements.txt
uvicorn main:app --host 0.0.0.0 --port 8001 --reload
```

**ML Inference:**
```bash
cd services/ml-inference
pip install -r requirements.txt
uvicorn main:app --host 0.0.0.0 --port 8002 --reload
```

**Risk Assessment:**
```bash
cd services/risk-assessment
pip install -r requirements.txt
uvicorn main:app --host 0.0.0.0 --port 8003 --reload
```

**Notification Service:**
```bash
cd services/notification-service
pip install -r requirements.txt
uvicorn main:app --host 0.0.0.0 --port 8004 --reload
```

**Frontend:**
```bash
cd frontend
npm install
npm start
```

### Database Management

**Initialize Database:**
```bash
docker-compose exec postgres psql -U postgres -d supply_chain -f /docker-entrypoint-initdb.d/init.sql
```

**Connect to Database:**
```bash
docker-compose exec postgres psql -U postgres -d supply_chain
```

**View Tables:**
```sql
\dt
SELECT * FROM supply_chain_events LIMIT 5;
SELECT * FROM risk_assessments LIMIT 5;
```

### Message Queue Management

**Access RabbitMQ Management UI:**
- URL: http://localhost:15672
- Username: admin
- Password: admin

**Monitor Queues:**
- View message queues and exchanges
- Monitor message flow between services
- Debug event processing

## API Documentation

### Core Endpoints

**Dashboard:**
- `GET /api/dashboard/overview` - Get dashboard overview
- `GET /api/events/recent` - Get recent events
- `GET /api/risk-assessments/recent` - Get recent risk assessments

**Business Profiles:**
- `GET /api/business-profiles` - List all profiles
- `POST /api/business-profiles` - Create new profile

**Data Collection:**
- `POST /api/data-collection/trigger` - Trigger data collection

**ML Inference:**
- `POST /api/ml-inference/predict` - Get ML predictions

**Risk Assessment:**
- `POST /api/risk-assessment/analyze` - Analyze risk scenarios

**Alerts:**
- `GET /api/alerts/active` - Get active alerts
- `POST /api/alerts/{id}/acknowledge` - Acknowledge alert
- `POST /api/alerts/{id}/resolve` - Resolve alert

### Health Checks

All services provide health check endpoints:
- `GET /health` - Service health status

## Configuration

### Environment Variables

**Database Configuration:**
```bash
DATABASE_URL=postgresql://postgres:postgres@postgres:5432/supply_chain
```

**Redis Configuration:**
```bash
REDIS_URL=redis://redis:6379
```

**RabbitMQ Configuration:**
```bash
RABBITMQ_URL=amqp://admin:admin@rabbitmq:5672/
```

**External APIs (Optional):**
```bash
NEWS_API_KEY=your-news-api-key
WEATHER_API_KEY=your-weather-api-key
ECONOMIC_API_KEY=your-economic-api-key
```

## Monitoring

### Service Health
Monitor service health through:
- Individual service `/health` endpoints
- Docker container status: `docker-compose ps`
- Service logs: `docker-compose logs [service-name]`

### Database Monitoring
```bash
# Check database connections
docker-compose exec postgres psql -U postgres -c "SELECT count(*) FROM pg_stat_activity;"

# Monitor table sizes
docker-compose exec postgres psql -U postgres -d supply_chain -c "
SELECT schemaname,tablename,attname,n_distinct,correlation FROM pg_stats;
"
```

### Message Queue Monitoring
- Access RabbitMQ Management UI at http://localhost:15672
- Monitor queue depths and message rates
- Check for failed messages

## Troubleshooting

### Common Issues

**Services not starting:**
```bash
# Check logs
docker-compose logs [service-name]

# Restart specific service
docker-compose restart [service-name]

# Rebuild and restart
docker-compose up --build [service-name]
```

**Database connection issues:**
```bash
# Check database is running
docker-compose exec postgres pg_isready -U postgres

# Reset database
docker-compose down -v
docker-compose up -d postgres
```

**Frontend not loading:**
```bash
# Check if API Gateway is running
curl http://localhost:8000/health

# Rebuild frontend
docker-compose build frontend
docker-compose up -d frontend
```

### Performance Optimization

**Database:**
- Monitor query performance
- Add indexes for frequently queried columns
- Use connection pooling

**Redis:**
- Monitor memory usage
- Set appropriate TTL values
- Use Redis clustering for scale

**Message Queue:**
- Monitor queue depths
- Adjust prefetch counts
- Use message acknowledgments

## Security

### Production Considerations

**Environment Variables:**
- Use secure secret management
- Rotate API keys regularly
- Use strong database passwords

**Network Security:**
- Use HTTPS in production
- Implement proper CORS policies
- Use VPN or private networks

**Database Security:**
- Enable SSL connections
- Use least privilege access
- Regular security updates

## Scaling

### Horizontal Scaling

**Microservices:**
- Scale individual services based on load
- Use load balancers for high availability
- Implement circuit breakers

**Database:**
- Use read replicas for read-heavy workloads
- Implement database sharding if needed
- Use connection pooling

**Message Queue:**
- Use RabbitMQ clustering
- Implement message persistence
- Monitor queue performance

### Vertical Scaling

**Resource Allocation:**
```yaml
# In docker-compose.yml
services:
  api-gateway:
    deploy:
      resources:
        limits:
          cpus: '2'
          memory: 2G
        reservations:
          cpus: '1'
          memory: 1G
```

## Backup and Recovery

### Database Backup
```bash
# Create backup
docker-compose exec postgres pg_dump -U postgres supply_chain > backup.sql

# Restore backup
docker-compose exec -T postgres psql -U postgres supply_chain < backup.sql
```

### Redis Backup
```bash
# Create Redis snapshot
docker-compose exec redis redis-cli BGSAVE

# Copy snapshot
docker cp $(docker-compose ps -q redis):/data/dump.rdb ./redis-backup.rdb
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request

### Development Guidelines

- Follow PEP 8 for Python code
- Use TypeScript for frontend development
- Write comprehensive tests
- Update documentation
- Use conventional commit messages

## License

This project is licensed under the MIT License - see the LICENSE file for details.
