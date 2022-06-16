FROM fishtownanalytics/dbt:0.20.2

ARG ssh_prv_key
ARG ssh_pub_key

ARG CLIENT

# client specific files
COPY ./clients/$CLIENT/ /usr/dbt
COPY deploy-requirements.txt /usr/dbt/

WORKDIR /usr/dbt/

# Authorize SSH Host
RUN mkdir -p /root/.ssh && \
    chmod 0700 /root/.ssh && \
    ssh-keyscan gitlab.com > /root/.ssh/known_hosts

# Add the keys and set permissions
RUN echo "$ssh_prv_key" > /root/.ssh/id_rsa && \
    echo "$ssh_pub_key" > /root/.ssh/id_rsa.pub && \
    chmod 600 /root/.ssh/id_rsa && \
    chmod 600 /root/.ssh/id_rsa.pub

RUN pip install -r deploy-requirements.txt

# install dbt dependencies
RUN dbt deps --profiles-dir /usr/app/profiles/
