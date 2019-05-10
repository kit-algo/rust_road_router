epsilons = [0.1, 0.5, 1.0, 5.0, 10.0]
thresholds = [100, 500, 1000, 5000, 10000]

epsilons.each do |epsilon|
  thresholds.each do |threshold|
    `TDCCH_APPROX=#{epsilon} TDCCH_APPROX_THRESHOLD=#{threshold} cargo run --release --features 'detailed-stats' --bin tdcch_customization -- /algoDaten/graphs/cleaned_td_road_data/ptv17-eur-car/day/di/ > ~/experiments/tdcch_vldb20/customization_approx/$(date --iso-8601=seconds).json`
  end
end
