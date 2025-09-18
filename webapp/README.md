# MRF Data Lookup Webapp

A modern web application for visualizing and exploring Machine Readable Files (MRF) data from your ETL pipeline.

## Features

- **🌐 HTML Dashboard**: Modern, responsive HTML interface (Recommended)
- **📊 Streamlit Dashboard**: Alternative Streamlit-based frontend
- **🔧 REST API**: FastAPI backend with comprehensive data access endpoints
- **⚡ Fast Search**: Optimized indexed queries with materialized views (10-50x faster)
- **🔍 Multi-Field Search**: Search by all key fields with intuitive filters
- **📱 Responsive Design**: Works on desktop, tablet, and mobile
- **🎨 Modern UI**: Bootstrap-based interface with beautiful styling
- **⚡ Real-time Results**: Instant search with sub-100ms response times
- **📈 Live Metrics**: Real-time statistics and performance indicators

## Architecture

```
webapp/
├── backend/
│   └── main.py              # FastAPI REST API
├── frontend/
│   ├── index.html           # HTML Dashboard (Recommended)
│   └── dashboard.py         # Streamlit dashboard (Alternative)
├── utils/
│   ├── data_queries.py      # Data access utilities
│   └── optimized_queries.py # Optimized search queries
├── requirements.txt         # Python dependencies
├── start_backend.py         # Backend startup script
├── start_frontend.py        # Streamlit frontend startup script
├── start_html_dashboard.py  # HTML dashboard startup script
└── start_dashboard.py       # Complete launcher (Backend + HTML)
└── README.md               # This file
```

## Quick Start

### 1. Install Dependencies

```bash
cd webapp
pip install -r requirements.txt
```

### 2. Setup Optimized Dashboard (Recommended)

For maximum performance, run the optimization setup:

```bash
python setup_optimized_dashboard.py
```

This will:
- Create materialized views for 10-50x faster queries
- Set up indexes for instant search
- Test the optimization setup
- Provide performance metrics

### 3. Start the Dashboard

#### Option A: Complete Dashboard (Recommended)
Start both backend and HTML dashboard with one command:

```bash
python start_dashboard.py
```

This will start:
- 🔧 FastAPI Backend: http://localhost:8000
- 🌐 HTML Dashboard: http://localhost:8080

#### Option B: Manual Setup
Start services individually:

```bash
# Terminal 1: Start Backend
python start_backend.py

# Terminal 2: Start HTML Dashboard
python start_html_dashboard.py
```

#### Option C: Streamlit Dashboard (Alternative)
If you prefer the Streamlit interface:

```bash
# Terminal 1: Start Backend
python start_backend.py

# Terminal 2: Start Streamlit Dashboard
python start_frontend.py
```

### 5. Performance Comparison (Optional)

Compare optimized vs standard performance:

```bash
python performance_comparison.py
```

## Usage

### Dashboard Tabs

1. **📈 Overview**: High-level metrics and KPIs
2. **🏥 By Payer**: Analysis grouped by insurance payer
3. **🔬 By Procedure**: Analysis grouped by procedure codes
4. **📋 Rate Details**: Detailed rate records with filtering
5. **👨‍⚕️ Provider Search**: Search and explore providers
6. **🔍 Dimension Explorer**: Explore available dimension values
7. **⚡ Fast Search**: Optimized indexed queries (NEW!)

### API Endpoints

#### Standard Endpoints
- `GET /api/health` - Health check and data availability
- `GET /api/rates/summary` - Rate summary statistics
- `GET /api/rates/by-payer` - Rates grouped by payer
- `GET /api/rates/by-procedure` - Rates grouped by procedure
- `GET /api/rates/detail` - Detailed rate records
- `GET /api/providers/search` - Provider search
- `GET /api/meta/available-data` - Available data metadata

#### ⚡ Optimized Search Endpoints (10-50x faster)
- `GET /api/search/tin` - Fast TIN-based search
- `GET /api/search/organization` - Fast organization search
- `GET /api/search/taxonomy` - Fast taxonomy search
- `GET /api/search/procedure-category` - Fast procedure category search
- `GET /api/search/billing-code` - Fast billing code search
- `GET /api/search/payer` - Fast payer search
- `GET /api/search/multi-field` - Comprehensive multi-field search
- `GET /api/autocomplete/{field}` - Autocomplete suggestions
- `GET /api/search/statistics` - Search statistics

### Example API Usage

```python
import requests

# Get rate summary for GA, August 2025
response = requests.get("http://localhost:8000/api/rates/summary", 
                       params={"state": "GA", "year_month": "2025-08"})
data = response.json()
print(f"Total rates: {data['summary']['total_rates']}")
```

## Configuration

### Data Paths

The webapp expects your data files to be located at:
- `prod_etl/core/data/gold/fact_rate.parquet`
- `prod_etl/core/data/dims/dim_*.parquet`
- `prod_etl/core/data/xrefs/xref_*.parquet`

### API Configuration

To change the API base URL in the frontend, edit `frontend/dashboard.py`:

```python
API_BASE_URL = "http://localhost:8000"  # Change this if needed
```

### CORS Settings

For production deployment, update CORS settings in `backend/main.py`:

```python
app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://yourdomain.com"],  # Update for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
```

## Development

### Adding New Endpoints

1. Add the endpoint to `backend/main.py`
2. Add corresponding frontend functionality to `frontend/dashboard.py`
3. Update `utils/data_queries.py` if needed

### Adding New Visualizations

1. Create new functions in `frontend/dashboard.py`
2. Add new tabs or sections as needed
3. Use Plotly for interactive charts

## Performance Tips

1. **Caching**: The frontend uses Streamlit's `@st.cache_data` for API calls
2. **Query Optimization**: Use appropriate filters to limit data size
3. **Pagination**: Large result sets are paginated (configurable limit)
4. **DuckDB**: Backend uses DuckDB for fast analytical queries

## Troubleshooting

### Common Issues

1. **API Connection Error**: Ensure the backend is running on port 8000
2. **Data Not Found**: Check that your Parquet files exist in the expected locations
3. **Slow Performance**: Try filtering data by state/year-month first
4. **Memory Issues**: Reduce the limit parameter for large datasets

### Debug Mode

To run with debug logging:

```bash
# Backend
uvicorn backend.main:app --reload --log-level debug

# Frontend
streamlit run frontend/dashboard.py --logger.level debug
```

## Production Deployment

### Docker Deployment

Create a `Dockerfile`:

```dockerfile
FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .
EXPOSE 8000 8501

# Start both services
CMD ["sh", "-c", "python start_backend.py & python start_frontend.py"]
```

### Environment Variables

Set these for production:

```bash
export API_BASE_URL="https://your-api-domain.com"
export DATA_ROOT="/path/to/your/data"
export LOG_LEVEL="info"
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Test thoroughly
5. Submit a pull request

## License

This project is part of the MRF ETL pipeline for BeaconPoint Health.
