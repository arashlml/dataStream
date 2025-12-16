# Elasticsearch cURL Cheatsheet (Windows CMD – Single Line)

This document contains **corrected, working one-line cURL commands** compatible with **Windows CMD** (no single quotes, escaped JSON).

---

## 1. Run Elasticsearch (Docker)

```bash
docker run -d --name elastic -p 9200:9200 -e "discovery.type=single-node" -e "ES_JAVA_OPTS=-Xms1g -Xmx1g" docker.elastic.co/elasticsearch/elasticsearch:8.19.8
```

---

## 2. Reset `elastic` User Password

```bash
bin/elasticsearch-reset-password -u elastic
```

---

## 3. Create Index & Mapping

### 3.1 Products Index Mapping

```bash
curl -u "elastic:mJquAU0Sa2cgYSyz2bnf" -X PUT "http://localhost:9200/products?pretty" -H "Content-Type: application/json" -d "{\"mappings\":{\"properties\":{\"id\":{\"type\":\"integer\"},\"name\":{\"type\":\"text\",\"analyzer\":\"standard\",\"fields\":{\"keyword\":{\"type\":\"keyword\"}}},\"product_variant\":{\"type\":\"nested\",\"properties\":{\"id\":{\"type\":\"integer\"},\"name\":{\"type\":\"text\",\"analyzer\":\"standard\",\"fields\":{\"keyword\":{\"type\":\"keyword\"}}}}}}}"
```

---

### 3.2 Users Index Mapping

```bash
curl -u "elastic:mJquAU0Sa2cgYSyz2bnf" -X PUT "http://localhost:9200/users?pretty" -H "Content-Type: application/json" -d "{\"mappings\":{\"properties\":{\"id\":{\"type\":\"integer\"},\"name\":{\"type\":\"text\",\"analyzer\":\"standard\",\"fields\":{\"keyword\":{\"type\":\"keyword\"}}},\"age\":{\"type\":\"integer\"},\"attribute\":{\"type\":\"object\"},\"fav\":{\"type\":\"keyword\"}}}}"
```

---

## 4. Search Queries

### 4.1 Search All Users (Pagination + Sort)

```bash
curl -X POST "http://localhost:9200/users/_search?pretty" -u "elastic:mJquAU0Sa2cgYSyz2bnf" -H "Content-Type: application/json" -d "{\"from\":0,\"size\":10,\"query\":{\"match_all\":{}},\"sort\":[{\"id.keyword\":{\"order\":\"asc\"}}]}"
```

---

### 4.2 Search User by Name

```bash
curl -X POST "http://localhost:9200/users/_search?pretty" -u "elastic:mJquAU0Sa2cgYSyz2bnf" -H "Content-Type: application/json" -d "{\"query\":{\"match\":{\"name\":\"Sara_90\"}}}"
```

---

### 4.3 Search Product by Name

```bash
curl -X POST "http://localhost:9200/products/_search?pretty" -u "elastic:mJquAU0Sa2cgYSyz2bnf" -H "Content-Type: application/json" -d "{\"query\":{\"match\":{\"name\":\"powerbank\"}}}"
```

---

### 4.4 Get First 10 Records

```bash
curl -X POST "http://localhost:9200/products/_search?pretty" -u "elastic:mJquAU0Sa2cgYSyz2bnf" -H "Content-Type: application/json" -d "{\"from\":0,\"size\":10,\"query\":{\"match_all\":{}}}"
```

---

## 5. Delete Data

### 5.1 Delete All Documents in `products`

```bash
curl -X POST "http://localhost:9200/products/_delete_by_query?pretty" -u "elastic:mJquAU0Sa2cgYSyz2bnf" -H "Content-Type: application/json" -d "{\"query\":{\"match_all\":{}}}"
```

---

## 6. Count Documents

```bash
curl -X GET "http://localhost:9200/products/_count?pretty" -u "elastic:mJquAU0Sa2cgYSyz2bnf"
```

---

## Notes

* All commands are **Windows CMD compatible**
* Elasticsearch runs over **HTTP (not HTTPS)**
* Security enabled → **Basic Auth required**
* `wrong version number` means using `https://` against HTTP

---

✅ Ready to save as `elasticsearch-queries.md`
