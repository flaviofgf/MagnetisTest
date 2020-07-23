FROM puckel/docker-airflow

RUN python -m pip install --upgrade pip

COPY requirements.txt .

RUN pip install --user -r requirements.txt

ENV HOST_MAIN_PATH 'D:/Projects/magnetis_test'