FROM python:alpine

# Working from /app
WORKDIR /app

# Install requirements
# Disable caching, to keep Docker image lean
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

ADD main.py .

CMD [ "python", "main.py" ]
