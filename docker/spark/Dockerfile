FROM jupyter/pyspark-notebook

RUN python -m pip install --upgrade pip

COPY requirements.txt .

RUN pip install --user -r requirements.txt