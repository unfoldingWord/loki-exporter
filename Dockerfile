FROM python:alpine

# Working from /app
WORKDIR /app

ADD main.py .

COPY requirements.txt .

# Install requirements
# Disable caching, to keep Docker image lean
RUN pip install --no-cache-dir -r requirements.txt

CMD [ "python", "main.py" ]
