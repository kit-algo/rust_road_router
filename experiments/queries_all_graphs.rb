Dir.glob('/algoDaten/graphs/cleaned_td_road_data/*/day/*/').each do |graph_dir|
  `cargo run --release --bin tdcch_pregen_uniform_queries -- #{graph_dir} > ~/experiments/tdcch_vldb20/queries_all_graphs/$(date --iso-8601=seconds).json`
  `cargo run --release --bin tdcch_pregen_rank_queries -- #{graph_dir} > ~/experiments/tdcch_vldb20/rank_queries_all_graphs/$(date --iso-8601=seconds).json`
end

