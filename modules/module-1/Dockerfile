FROM python:3.9-slim
WORKDIR /app
RUN pip install --no-cache-dir mysql-connector-python
COPY ./modules/module-1/adapter.py .
CMD ["python", "adapter.py"]