FROM node:20-alpine

WORKDIR /app

# 先拷贝依赖清单，利用镜像层缓存
COPY package.json package-lock.json ./
RUN npm ci --omit=dev

# 再拷贝应用代码
COPY server.js ./
COPY public ./public

ENV NODE_ENV=production
ENV PORT=3000

EXPOSE 3000
USER node

CMD ["node", "server.js"]
