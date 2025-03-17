FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1
ENV POETRY_VIRTUALENVS_CREATE=false

WORKDIR /app
RUN apt-get update && apt-get install -y \
    build-essential \
    curl

RUN curl -sSL https://install.python-poetry.org | python3 - \
    && ln -s ~/.local/bin/poetry /usr/local/bin/poetry
RUN poetry self add poetry-plugin-shell
COPY pyproject.toml /app/
COPY poetry.lock /app/
RUN poetry lock && poetry install --no-interaction --no-ansi --no-root

COPY . /app

