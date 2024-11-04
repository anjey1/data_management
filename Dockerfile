# Dockerfile
FROM node:18

WORKDIR /consumer_app
COPY package*.json ./
RUN npm install
COPY ./dist ./

# Run consumer_app
CMD ["node", "main.js", "consume"]