FROM python:3.8


WORKDIR /app


COPY ./requirements.txt /app/requirements.txt
RUN pip install -r requirements.txt


COPY ./etl.py /app/etl.py


COPY . /app/input


CMD ["python", "etl.py"]
