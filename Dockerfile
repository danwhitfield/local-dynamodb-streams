FROM node:18.12.1

RUN apt-get update && \
    apt-get install -y python3-pip && \
    pip install aws-sam-cli 

WORKDIR /app
COPY package.json /app/
COPY package-lock.json /app/
COPY index.ts /app/
RUN npm i

CMD ["npm", "start"]
