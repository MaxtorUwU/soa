FROM python:3.11-slim
WORKDIR /app
COPY servicio_usuarios.py /app
RUN pip install psycopg2-binary
CMD ["python", "servicio_usuarios.py"]
