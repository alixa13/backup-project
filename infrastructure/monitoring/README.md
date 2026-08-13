# infrastructure/monitoring/

`prometheus.yml`, `alerts.yml`, `grafana-dashboard.json`: bounded-cardinality
metrics scrape config, alert rules, and the four MVP dashboards (see Roadmap.md
Day 10-12). Never use IP address or event ID as a metric label. Not yet committed —
Step 1 scope is directory skeleton only.
