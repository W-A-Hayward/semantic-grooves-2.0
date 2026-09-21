# Semantic-grooves

## What it is

semantic-grooves is a music discovery engine that recommends albums by **mood and emotion** rather than by genre. Instead of asking "more like this artist," it lets you search by how you want to feel — melancholic, driving, hazy, triumphant — and surfaces albums whose reviews carry that vibe, using LLM-generated mood/genre tags and vector embeddings over a corpus of album reviews.

## Why refactor into a distributed system

The original version was a single-machine Python implementation (SQLite + Ollama for embeddings, MMR ranking and vector search). It works, but it doesn't demonstrate distributed systems engineering.

This refactor exists as a deliberate learning vehicle, prompted by working through **"Understanding Distributed Systems: What Every Developer Should Know About Large-Scale Data Systems" by Roberto Vitillo**. The goal is to rebuild the same product on a real distributed stack — Go, Kafka, and Kubernetes — with hand-rolled distributed primitives (custom sharding, scatter-gather queries) instead of off-the-shelf vector databases, in order to build and demonstrate hands-on implementation depth rather than just conceptual knowledge.

**Architecture at a glance:**
- **Offline bootstrap pipeline** — a Kubernetes `Job` on GPU nodes that ingests album reviews, groups them per album into Kafka messages, runs a local LLM to generate mood/genre tags and vibe summaries, embeds them, and builds the search index before launch.
- **Online serving layer** — standing Kubernetes `Deployments`: a tagging/embedding service (autoscaled on Kafka consumer lag) and a Go Search API (autoscaled on request volume).

## Roadmap

- [x] **Phase 0 — Design & local dev setup**
  - [x] Lock in architecture decisions
  - [x] Set up local Kubernetes/Kafka dev environment

- [ ] **Phase 1 — Bootstrap pipeline (end-to-end)**
  - [x] Ingestion service
  - [x] LLM prompt schema for mood/genre tagging
  - [ ] GPU worker throughput sizing
  - [ ] Idempotency guarantees
  - [ ] Embedding generation and index build

- [ ] **Phase 2 — Search API & frontend**
  - [ ] Go Search API
  - [ ] Frontend for mood-based search

- [ ] **Phase 3 — Live review loop**
  - [ ] Click-gated review ingestion
  - [ ] Immediate re-tagging on new reviews

- [ ] **Phase 4 — Containerization**
  - [ ] Dockerize all services

- [ ] **Phase 5 — Scaling**
  - [ ] HPA tuning for search API and tagging service
  - [ ] Kafka partitioning validation under load

- [ ] **Phase 6 — Hardening & polish**
  - [ ] Observability (Prometheus)
  - [ ] Failure handling and resilience testing
  - [ ] Final cleanup
