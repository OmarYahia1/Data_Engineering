FROM python:3.11


WORKDIR /m2_docker

RUN pip install pandas sqlalchemy psycopg2 scikit-learn requests beautifulsoup4

ENTRYPOINT ["python","src/main.py"]


