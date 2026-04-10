wrk.method = "POST"
wrk.body = '{"executeAt":"2026-04-10T00:00:00Z","deadline":"2026-04-10T01:00:00Z","precisionTier":"STANDARD","shardKey":"bench","callback":{"url":"http://localhost/webhook"}}'
wrk.headers["Content-Type"] = "application/json"
