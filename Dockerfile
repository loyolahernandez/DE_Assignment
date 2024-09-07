# Usa una imagen base de Python
FROM python:3.9

# Establece el directorio de trabajo
WORKDIR /app

# Copia los archivos de tu proyecto
COPY . /app

# Instala las dependencias necesarias
RUN pip install -r requirements.txt

# Ejecuta el script de Python
CMD ["python", "main.py"]
