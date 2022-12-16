FROM bitnami/spark:3.3.0
USER root

# Installing package into Spark if needed
# spark-shell --master local --packages "<package name>"
RUN pip install findspark
RUN pip install pandas==1.4.3
