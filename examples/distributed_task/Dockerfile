# Dockerfile
FROM python:3.11

COPY requirements.txt /app/requirements.txt
RUN pip install -r /app/requirements.txt
RUN python3 -m pip install --index-url https://test.pypi.org/simple/ aiodistributor

COPY performance_test.py /app/performance_test.py

WORKDIR /app
CMD ["python", "performance_test.py"]
