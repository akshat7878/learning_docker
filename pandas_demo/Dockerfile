FROM python:slim

WORKDIR /app

COPY requirements.txt .

RUN pip install -r requirements.txt

COPY demo.py .

CMD ["python", "demo.py"]