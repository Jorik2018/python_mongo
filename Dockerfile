FROM python:3.11

# Install MongoDB tools
RUN apt-get update && apt-get install -y mongodb-tools

# Install Poetry
RUN pip install poetry

# Set the working directory in the container
WORKDIR /app

# Copy only the pyproject.toml and poetry.lock to leverage Poetry's caching mechanism
COPY pyproject.toml poetry.lock /app/

# Install dependencies using Poetry
RUN poetry install --no-root

# Copy the rest of your application code
COPY . /app

# Command to run your application
CMD ["poetry", "run", "python", "your_script.py"]