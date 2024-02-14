FROM python:3.11

# Install MongoDB tools
RUN wget https://fastdl.mongodb.org/tools/db/mongodb-database-tools-debian92-x86_64-100.3.1.deb && \
    apt install ./mongodb-database-tools-*.deb && \
    rm -f mongodb-database-tools-*.deb
	
# Install Poetry
RUN pip install poetry==1.7.1

WORKDIR /app

COPY pyproject.toml poetry.lock ./

COPY mongodb_pandas_project ./mongodb_pandas_project

RUN touch README.md

RUN poetry install --no-root --without dev

# Copy the rest of your application code
COPY . /app

# Command to run your application
CMD ["poetry", "run", "python","-m", "mongodb_pandas_project.main"]