FROM python:3.13-slim
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /code/

# install gcc to be able to build packages - e.g. required by regex, dateparser
RUN apt-get update && apt-get install -y build-essential && rm -rf /var/lib/apt/lists/*

# Copy dependency files first (layer caching)
COPY pyproject.toml .
COPY uv.lock .

# Install dependencies into system Python (no venv in Docker)
ENV UV_PROJECT_ENVIRONMENT="/usr/local/"
RUN uv sync --all-groups --frozen

# Copy source code
COPY src/ src
COPY tests/ tests
COPY scripts/ scripts

CMD ["python", "-u", "src/component.py"]
