use super::*;
use rayon::prelude::*;
use std::cmp::Reverse;
use std::collections::BinaryHeap;

pub fn merge(metrics: &[&[Weight]], target_size: usize) -> Vec<Vec<usize>> {
    let m = metrics[0].len();
    for metric in metrics {
        assert_eq!(metric.len(), m);
    }
    let mut queue = BinaryHeap::new();
    let mut merged: Vec<_> = (0..metrics.len()).map(|idx| vec![idx]).collect();
    let mut remaining_metrics = metrics.len();

    for (idx, metric) in metrics.iter().enumerate() {
        for (other_idx, other_metric) in metrics.iter().enumerate() {
            if other_idx > idx {
                let sum_of_squared_diffs: u64 = metric
                    .par_iter()
                    .zip(other_metric.par_iter())
                    .map(|(&w1, &w2)| w1.abs_diff(w2) as u64 * w1.abs_diff(w2) as u64)
                    .sum();
                queue.push(Reverse((sum_of_squared_diffs, idx, other_idx)));
            }
        }
    }

    while remaining_metrics > target_size {
        if queue.is_empty() {
            break;
        }
        let Reverse((_, idx1, idx2)) = queue.pop().unwrap();
        remaining_metrics -= 1;
        let mut tmp = Vec::new();
        std::mem::swap(&mut tmp, &mut merged[idx2]);
        merged[idx1].append(&mut tmp);

        queue.retain(|&Reverse((_, other_idx1, other_idx2))| other_idx1 != idx1 && other_idx1 != idx2 && other_idx2 != idx1 && other_idx2 != idx2);

        for (other_idx, metric_group) in merged.iter().enumerate() {
            if other_idx == idx1 || metric_group.is_empty() {
                continue;
            }

            let sum_of_squared_diffs: u64 = (0..m)
                .into_par_iter()
                .map(|edge_idx| {
                    let w1 = merged[idx1].iter().map(|&idx| metrics[idx][edge_idx]).min().unwrap();
                    let w2 = metric_group.iter().map(|&idx| metrics[idx][edge_idx]).min().unwrap();
                    w1.abs_diff(w2) as u64 * w1.abs_diff(w2) as u64
                })
                .sum();
            if idx1 < other_idx {
                queue.push(Reverse((sum_of_squared_diffs, idx1, other_idx)));
            } else {
                queue.push(Reverse((sum_of_squared_diffs, other_idx, idx1)));
            }
        }
    }

    merged.retain(|group| !group.is_empty());

    merged
}
