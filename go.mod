module test-provider

go 1.12

require (
	github.com/google/uuid v1.1.1 // indirect
	github.com/synerex/synerex_api v0.3.1
	github.com/synerex/synerex_proto v0.1.6
	github.com/synerex/synerex_sxutil v0.4.10
)

replace(
	github.com/RuiHirano/synerex_simulation_beta/api/sim_proto => ./sim_proto
)