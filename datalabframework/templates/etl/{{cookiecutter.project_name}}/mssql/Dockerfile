FROM microsoft/mssql-server-linux:latest

# Install app dependencies
COPY data /usr/data

# Grant permissions for the import-data script to be executable
RUN chmod +x /usr/data/import-data.sh
RUN chmod +x /usr/data/entrypoint.sh

WORKDIR /usr/data

EXPOSE 1433

CMD /bin/bash ./entrypoint.sh
