Dir.glob('/algoDaten/graphs/cleaned_td_road_data/*/day/*/').each do |graph_dir|
  `cargo run --release --features 'tdcch-approx' --bin tdcch_static_preprocessing -- #{graph_dir}`
end
