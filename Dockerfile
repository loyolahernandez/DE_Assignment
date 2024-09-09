# Python Image
FROM python:3.9

# Set the working directory
WORKDIR /app

# Copy the files of your project
COPY . /app

# Install the necessary dependencies
RUN pip install -r requirements.txt

# Execute the Python script
CMD ["python", "main.py"]