# Image Airflow 3.3.2 + bibliothèques des TP (M2 Data Science en santé, Lille)
FROM apache/airflow:3.3.2
COPY requirements.txt /requirements.txt
# On repasse apache-airflow==3.3.2 pour que pip ne le modifie jamais en installant le reste
RUN pip install --no-cache-dir "apache-airflow==3.3.2" -r /requirements.txt
