FROM python:3.12-slim

WORKDIR /z2m_iot

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY z2m-decoder.py .

CMD ["python", "z2m-decoder.py"]