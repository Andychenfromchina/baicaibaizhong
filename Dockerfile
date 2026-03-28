FROM nginx:alpine
COPY nginx.conf /etc/nginx/conf.d/default.conf
COPY lottery-prediction.html /usr/share/nginx/html/lottery-prediction.html
EXPOSE 88
