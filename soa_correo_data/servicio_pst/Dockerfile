FROM python:3.11-slim
WORKDIR /app
COPY servicio_pst.py /app
RUN pip install psycopg2-binary
CMD ["python", "servicio_pst.py"]
