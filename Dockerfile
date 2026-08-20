FROM apache/airflow:2.9.3

COPY requirements.txt /requirements.txt

# --user isn't needed here since the airflow image already runs pip installs
# as the airflow user by convention; just install directly.
RUN pip install --no-cache-dir -r /requirements.txt
