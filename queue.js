'use strict';
/**
 * queue.js — Pure in-memory job queue with concurrency control.
 * No Redis, no Docker, no external services required.
 */

const { EventEmitter } = require('events');

class InMemoryQueue extends EventEmitter {
    constructor({ concurrency = 3, maxJobAge = 3600000 } = {}) {
        super();
        this.concurrency  = concurrency;
        this.maxJobAge    = maxJobAge; // ms before completed jobs are pruned (default 1 hour)
        this.jobs         = new Map(); // id -> job object
        this.pending      = [];        // queue of { id, handler }
        this.running      = 0;
        // Prune old jobs every 10 minutes
        setInterval(() => this._prune(), 600000).unref();
    }

    /** Add a job. Returns the job ID immediately. */
    add(name, data, handler) {
        const id  = `${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
        const job = {
            id,
            name,
            data,
            status:    'waiting',   // waiting | active | completed | failed
            progress:  0,
            result:    null,
            error:     null,
            createdAt: new Date().toISOString(),
            updatedAt: new Date().toISOString()
        };
        this.jobs.set(id, job);
        this.pending.push({ id, handler, data });
        setImmediate(() => this._tick());
        return id;
    }

    /** Get a single job by ID. */
    getJob(id) {
        return this.jobs.get(id) || null;
    }

    /** Update concurrency dynamically and trigger tick if increased */
    setConcurrency(n) {
        const val = parseInt(n, 10);
        if (!isNaN(val) && val > 0) {
            this.concurrency = val;
            this._tick();
        }
    }

    /** Stats snapshot for the dashboard. */
    getStats() {
        const stats = { waiting: 0, active: 0, completed: 0, failed: 0, total: this.jobs.size, concurrency: this.concurrency, running: this.running };
        for (const j of this.jobs.values()) stats[j.status] = (stats[j.status] || 0) + 1;
        return stats;
    }

    /** Get recent jobs for the dashboard (newest first). */
    getJobs(limit = 50) {
        return [...this.jobs.values()]
            .sort((a, b) => b.createdAt.localeCompare(a.createdAt))
            .slice(0, limit);
    }

    _tick() {
        while (this.running < this.concurrency && this.pending.length > 0) {
            const { id, handler, data } = this.pending.shift();
            const job = this.jobs.get(id);
            if (!job) continue;

            job.status    = 'active';
            job.updatedAt = new Date().toISOString();
            this.running++;

            const updateProgress = (pct) => {
                if (job) { job.progress = pct; job.updatedAt = new Date().toISOString(); }
            };

            handler(data, updateProgress)
                .then(result => {
                    job.status    = 'completed';
                    job.result    = result;
                    job.progress  = 100;
                    job.updatedAt = new Date().toISOString();
                    this.emit('completed', job, result);
                })
                .catch(err => {
                    job.status    = 'failed';
                    job.error     = err.message;
                    job.updatedAt = new Date().toISOString();
                    this.emit('failed', job, err);
                })
                .finally(() => {
                    this.running--;
                    this._tick();
                });
        }
    }

    _prune() {
        const cutoff = Date.now() - this.maxJobAge;
        for (const [id, job] of this.jobs) {
            if ((job.status === 'completed' || job.status === 'failed') &&
                new Date(job.updatedAt).getTime() < cutoff) {
                this.jobs.delete(id);
            }
        }
    }
}

module.exports = InMemoryQueue;
