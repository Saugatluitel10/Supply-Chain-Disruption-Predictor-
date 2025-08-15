#!/bin/bash

# Supply Chain Predictor - 3-Tier Microservices Startup Script

echo "🚀 Starting Supply Chain Predictor - 3-Tier Microservices Architecture"
echo "=================================================================="

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "❌ Docker is not running. Please start Docker and try again."
    exit 1
fi

# Check if Docker Compose is available
if ! command -v docker-compose &> /dev/null; then
    echo "❌ Docker Compose is not installed. Please install Docker Compose and try again."
    exit 1
fi

# Create .env file if it doesn't exist
if [ ! -f .env ]; then
    echo "📝 Creating .env file from template..."
    cp .env.example .env 2>/dev/null || echo "⚠️  Please create .env file manually from .env.example"
fi

# Stop any existing containers
echo "🛑 Stopping existing containers..."
docker-compose down

# Build and start all services
echo "🏗️  Building and starting all services..."
docker-compose up -d --build

# Wait for services to be ready
echo "⏳ Waiting for services to start..."
sleep 30

# Check service health
echo "🔍 Checking service health..."

services=("postgres:5432" "redis:6379" "rabbitmq:5672" "api-gateway:8000" "data-collector:8001" "ml-inference:8002" "risk-assessment:8003" "notification-service:8004" "frontend:3000")

for service in "${services[@]}"; do
    IFS=':' read -r name port <<< "$service"
    if docker-compose exec -T $name echo "Service $name is running" > /dev/null 2>&1; then
        echo "✅ $name service is running"
    else
        echo "❌ $name service failed to start"
    fi
done

echo ""
echo "🎉 Supply Chain Predictor is now running!"
echo "=================================================================="
echo "📊 Frontend Dashboard:     http://localhost:3000"
echo "🔗 API Gateway:            http://localhost:8000"
echo "🐰 RabbitMQ Management:    http://localhost:15672 (admin/admin)"
echo "📈 Health Check:           http://localhost:8000/health"
echo ""
echo "🔧 Individual Services:"
echo "   • Data Collector:       http://localhost:8001"
echo "   • ML Inference:         http://localhost:8002"
echo "   • Risk Assessment:      http://localhost:8003"
echo "   • Notification Service: http://localhost:8004"
echo ""
echo "📚 Documentation:"
echo "   • Architecture:         ./ARCHITECTURE.md"
echo "   • Deployment Guide:     ./DEPLOYMENT.md"
echo "   • API Documentation:    http://localhost:8000/docs"
echo ""
echo "🛠️  Useful Commands:"
echo "   • View logs:            docker-compose logs -f [service-name]"
echo "   • Stop services:        docker-compose down"
echo "   • Restart service:      docker-compose restart [service-name]"
echo "   • Database shell:       docker-compose exec postgres psql -U postgres -d supply_chain"
echo ""
echo "Happy monitoring! 🎯"
