FROM python:3.11

# Install MongoDB tools
RUN wget https://fastdl.mongodb.org/tools/db/mongodb-database-tools-debian92-x86_64-100.3.1.deb && \
    apt install ./mongodb-database-tools-*.deb && \
    rm -f mongodb-database-tools-*.deb
	
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
CMD ["poetry", "run", "python", "mongodb_pandas_project/main.py","test"]