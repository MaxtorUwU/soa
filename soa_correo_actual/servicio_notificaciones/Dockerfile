# Use an official Python runtime as a parent image
FROM python:3.11-slim

# Set the working directory in the container
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY servicio_notificaciones.py /app

# Install any needed packages specified in requirements.txt
RUN pip install psycopg2-binary

# Run servicio_notificaciones.py when the container launches
CMD ["python", "servicio_notificaciones.py"]

