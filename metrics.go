package rink

import "github.com/prometheus/client_golang/prometheus"

var (
	leaderGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "rink",
		Subsystem: "cluster",
		Name:      "leader_bool",
		Help:      "Whether or not this instance is the cluster leader",
	}, []string{"cluster"})

	rankGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "rink",
		Subsystem: "cluster",
		Name:      "rank",
		Help:      "Rank of this instance in the cluster (set to -1 if no rank)",
	}, []string{"cluster"})

	roleGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "rink",
		Subsystem: "scheduler",
		Name:      "role_bool",
		Help:      "Whether or not this instance has a role",
	}, []string{"cluster", "role"})
)

func init() {
	prometheus.MustRegister(
		leaderGauge,
		rankGauge,
		roleGauge)
}
