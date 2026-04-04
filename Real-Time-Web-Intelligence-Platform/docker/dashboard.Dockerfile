FROM python:3.10

WORKDIR /app

COPY . /app

RUN pip install --no-cache-dir \
    streamlit \
    pandas \
    requests \
    streamlit-autorefresh

EXPOSE 8501

CMD ["streamlit", "run", "dashboard/app.py", "--server.port=8501", "--server.address=0.0.0.0"]