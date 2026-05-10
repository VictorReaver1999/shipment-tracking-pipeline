FROM python:3.11-slim

WORKDIR /app

# Install dependencies directly
RUN pip install streamlit psycopg2-binary pandas python-dotenv --no-cache-dir

# Copy app
COPY app.py .

# Run streamlit
CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]