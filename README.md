# Calo Balance Sync Log Analyzer

A comprehensive data engineering solution for analyzing AWS Lambda balance sync logs to detect overdrafts and generate automated reports.

## 🚀 Features

- **Multi-format File Support**: Process .log, .txt, .gz, .zip, .doc, .docx files
- **Batch Processing**: Handle zip archives containing multiple compressed log files
- **Real-time Overdraft Detection**: Identify subscribers with negative balances
- **Interactive Web Dashboard**: User-friendly interface for non-technical users
- **Automated Report Generation**: Export analysis to Excel and JSON formats
- **Docker Containerization**: Easy deployment and scaling
- **Enterprise-scale Processing**: Handle hundreds of log files efficiently

## 🏗️ Architecture

```
├── src/                    # Core processing modules
│   ├── log_parser.py      # Multi-format log parsing engine
│   ├── analyzer.py        # Data analysis and visualization
│   ├── balance_tracker.py # Overdraft detection system
│   └── report_generator.py # Excel/JSON report creation
├── web/                   # Flask web application
│   ├── app.py            # Main web server
│   └── templates/        # HTML templates
├── data/                 # Data storage
│   ├── logs/            # Raw log files
│   └── uploads/         # Uploaded files
├── reports/             # Generated reports
├── tests/               # Unit tests
└── docker-compose.yml   # Container orchestration
```

## 🛠️ Technology Stack

- **Python 3.9+**: Core language
- **Flask**: Web framework
- **Pandas**: Data processing and analysis
- **Plotly**: Interactive visualizations
- **Docker**: Containerization
- **Bootstrap**: Frontend UI framework

## 📋 Prerequisites

- Docker and Docker Compose
- Python 3.9+ (for local development)
- Git

## 🚀 Quick Start

### Using Docker (Recommended)

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd Calo_DE_project
   ```

2. **Start the application**
   ```bash
   docker-compose up -d
   ```

3. **Access the dashboard**
   - Open your browser and navigate to: `http://localhost:9000`

### Local Development

1. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

2. **Run the web application**
   ```bash
   cd web
   python app.py
   ```

## 📊 Usage

### Upload Files
1. Navigate to the web dashboard
2. Upload log files in any supported format:
   - Single files: `.log`, `.txt`, `.gz`, `.doc`, `.docx`
   - Batch processing: `.zip` archives containing multiple files

### View Analysis
- **Dashboard**: Real-time overview of transaction volumes and overdrafts
- **Reports**: Download detailed Excel/JSON reports
- **Visualizations**: Interactive charts and graphs

### Overdraft Alerts
The system automatically detects and alerts for:
- Subscribers with negative balances
- Potential overdraft patterns
- Balance sync failures

## 🔧 Configuration

### Environment Variables
- `FLASK_ENV`: Set to `development` or `production`
- `LOG_LEVEL`: Logging level (DEBUG, INFO, WARNING, ERROR)

### Docker Configuration
The application runs on port 9000 by default. Modify `docker-compose.yml` to change ports or add environment variables.

## 📈 Supported Log Formats

The system processes AWS Lambda logs with patterns including:
- Timestamp extraction
- Request ID tracking
- Subscriber identification
- Transaction amounts
- Status codes
- Duration measurements

## 🧪 Testing

Run the test suite:
```bash
python -m pytest tests/
```

## 📝 Report Generation

The system generates comprehensive reports including:
- Transaction volume analysis
- Overdraft patterns and trends
- Subscriber behavior insights
- Timeline analysis
- Performance metrics

## 🐛 Troubleshooting

### Common Issues

1. **Port 9000 already in use**
   - Change the port in `docker-compose.yml`
   - Kill processes using port 9000

2. **File upload errors**
   - Check file format is supported
   - Ensure file size is within limits
   - Verify Docker container has sufficient memory

3. **Processing errors**
   - Check log files for detailed error messages
   - Verify log format matches expected patterns

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request

## 📄 License

This project is proprietary software for Calo internal use.

## 🔒 Security

- This repository contains proprietary Calo code
- Do not share access with unauthorized personnel
- Follow company security guidelines for code handling

## 📞 Support

For technical support or questions:
- Create an issue in this repository
- Contact the Data Engineering team
- Refer to the implementation documentation

## 🏷️ Version History

- **v1.0.0**: Initial release with basic log parsing
- **v1.1.0**: Added multi-format file support
- **v1.2.0**: Implemented zip archive processing
- **v1.3.0**: Enhanced overdraft detection and reporting
