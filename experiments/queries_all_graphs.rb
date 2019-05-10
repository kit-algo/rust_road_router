Dir.glob('/algoDaten/graphs/cleaned_td_road_data/*/day/*/').each do |graph_dir|
  `RUSTFLAGS='-C target-cpu=native' cargo run --release --bin tdcch_pregen_uniform_queries -- #{graph_dir} > ~/experiments/tdcch_vldb20/queries_all_graphs/$(date --iso-8601=seconds).json`
  `RUSTFLAGS='-C target-cpu=native' cargo run --release --bin tdcch_pregen_rank_queries -- #{graph_dir} > ~/experiments/tdcch_vldb20/rank_queries_all_graphs/$(date --iso-8601=seconds).json`

  `RUSTFLAGS='-C target-cpu=native' cargo run --release --features 'detailed-stats' --bin tdcch_pregen_uniform_queries -- #{graph_dir} > ~/experiments/tdcch_vldb20/query_stats_all_graphs/$(date --iso-8601=seconds).json`
  `RUSTFLAGS='-C target-cpu=native' cargo run --release --features 'detailed-stats' --bin tdcch_pregen_rank_queries -- #{graph_dir} > ~/experiments/tdcch_vldb20/rank_query_stats_all_graphs/$(date --iso-8601=seconds).json`

  `RUSTFLAGS='-C target-cpu=native' cargo run --release --no-default-features --features 'detailed-stats' --bin tdcch_pregen_uniform_queries -- #{graph_dir} > ~/experiments/tdcch_vldb20/query_stats_no_astar_all_graphs/$(date --iso-8601=seconds).json`
  `RUSTFLAGS='-C target-cpu=native' cargo run --release --no-default-features --features 'detailed-stats' --bin tdcch_pregen_rank_queries -- #{graph_dir} > ~/experiments/tdcch_vldb20/rank_query_stats_no_astar_all_graphs/$(date --iso-8601=seconds).json`
end

