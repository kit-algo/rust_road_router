5.times do
  Dir.glob('/algoDaten/graphs/cleaned_td_road_data/*/day/*/').each do |graph_dir|
    `cargo run --release --bin tdcch_customization -- #{graph_dir} > ~/experiments/tdcch_vldb20/customization_all_graphs/$(date --iso-8601=seconds).json`
  end
end

