FROM python:3.13.11-slim
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/

WORKDIR /code

# Add virtual environment to PATH so we can use installed packages
ENV PATH="/app/.venv/bin:$PATH"

COPY .python-version uv.lock pyproject.toml ./
RUN uv sync --locked

RUN pip install pandas pyarrow

COPY pipeline.py .

ENTRYPOINT ["python", "pipeline.py"]