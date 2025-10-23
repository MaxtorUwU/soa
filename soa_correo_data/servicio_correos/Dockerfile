FROM python:3.11-slim
WORKDIR /app
COPY servicio_correos.py /app
RUN pip install requests
CMD ["python", "servicio_correos.py"]
