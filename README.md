# Supply Chain Disruption Predictor

An AI-powered system that analyzes global news, weather patterns, and economic indicators to predict supply chain bottlenecks and help small businesses prepare for disruptions.

## Features

- **Multi-Source Data Analysis**: Integrates news feeds, weather data, and economic indicators
- **AI-Powered Predictions**: Uses machine learning to identify potential supply chain disruptions
- **Business Impact Assessment**: Evaluates how disruptions might affect specific industries
- **Early Warning System**: Provides alerts and recommendations for small businesses
- **Interactive Dashboard**: User-friendly interface for monitoring and insights

## System Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Data Sources  │    │  AI/ML Engine   │    │  User Interface │
│                 │    │                 │    │                 │
│ • News APIs     │───▶│ • NLP Analysis  │───▶│ • Web Dashboard │
│ • Weather APIs  │    │ • Risk Scoring  │    │ • Alerts        │
│ • Economic APIs │    │ • Predictions   │    │ • Reports       │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

## Installation

1. Clone the repository
2. Install dependencies: `pip install -r requirements.txt`
3. Set up environment variables (see `.env.example`)
4. Run the application: `python app.py`

## Usage

1. Configure your business profile and supply chain dependencies
2. Monitor the dashboard for real-time risk assessments
3. Receive alerts about potential disruptions
4. Access detailed reports and recommendations

## Data Sources

- **News**: Global news feeds for supply chain events
- **Weather**: Severe weather patterns affecting logistics
- **Economic**: Trade data, port congestion, fuel prices
- **Geopolitical**: Political events affecting trade routes

## License

MIT License
