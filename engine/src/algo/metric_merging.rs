use super::*;
use crate::report::*;
use std::{
    cmp::Reverse,
    collections::BinaryHeap,
    sync::atomic::{AtomicU64, Ordering::SeqCst},
};

pub fn merge(metrics: &[&[Weight]], target_size: usize) -> Vec<Vec<usize>> {
    report!("num_threads", rayon::current_num_threads());

    let m = metrics[0].len();
    for metric in metrics {
        assert_eq!(metric.len(), m);
    }
    let mut queue = BinaryHeap::new();
    let mut merged: Vec<_> = (0..metrics.len()).map(|idx| vec![idx]).collect();
    let mut remaining_metrics = metrics.len();

    for (idx, _metric) in metrics.iter().enumerate() {
        for (other_idx, _other_metric) in metrics.iter().enumerate() {
            if other_idx > idx {
                queue.push(Reverse((0, 0, idx, other_idx)));
            }
        }
    }

    while remaining_metrics > target_size {
        if queue.is_empty() {
            break;
        }

        let &Reverse((_, edges_checked, idx1, idx2)) = queue.peek().unwrap();
        let diff_final = edges_checked as usize >= m;

        if diff_final {
            queue.pop();
            remaining_metrics -= 1;
            let mut tmp = Vec::new();
            std::mem::swap(&mut tmp, &mut merged[idx2]);
            merged[idx1].append(&mut tmp);

            queue.retain(|&Reverse((_, _, other_idx1, other_idx2))| other_idx1 != idx1 && other_idx1 != idx2 && other_idx2 != idx1 && other_idx2 != idx2);

            for (other_idx, metric_group) in merged.iter().enumerate() {
                if other_idx == idx1 || metric_group.is_empty() {
                    continue;
                }

                if idx1 < other_idx {
                    queue.push(Reverse((0, 0, idx1, other_idx)));
                } else {
                    queue.push(Reverse((0, 0, other_idx, idx1)));
                }
            }
        } else {
            let mut min_final_key = u64::MAX;
            let mut to_compute = Vec::new();
            while let Some(&Reverse((key, edges_checked, idx1, idx2))) = queue.peek() {
                if edges_checked as usize >= m {
                    min_final_key = key;
                    break;
                } else {
                    to_compute.push((key, edges_checked, idx1, idx2));
                    queue.pop();
                }
            }

            let min_final_key = AtomicU64::new(min_final_key);
            let queue = std::sync::Mutex::new(&mut queue);

            rayon::scope(|s| {
                for (sum_so_far, edges_checked_so_far, idx1, idx2) in to_compute {
                    let min_final_key = &min_final_key;
                    let queue = &queue;
                    let merged = &merged;
                    s.spawn(move |_| {
                        let mut sum_of_squared_diffs: u64 = sum_so_far;
                        let mut edges_checked = edges_checked_so_far;
                        for edge_idx in edges_checked_so_far as usize..m {
                            if sum_of_squared_diffs > min_final_key.load(SeqCst) {
                                break;
                            }
                            let w1 = merged[idx1].iter().map(|&idx| metrics[idx][edge_idx]).min().unwrap();
                            let w2 = merged[idx2].iter().map(|&idx| metrics[idx][edge_idx]).min().unwrap();
                            sum_of_squared_diffs += w1.abs_diff(w2) as u64 * w1.abs_diff(w2) as u64;
                            edges_checked += 1;
                        }
                        if edges_checked as usize >= m {
                            min_final_key.fetch_min(sum_of_squared_diffs, SeqCst);
                        }
                        queue.lock().unwrap().push(Reverse((sum_of_squared_diffs, edges_checked, idx1, idx2)));
                    });
                }
            });
        }
    }

    merged.retain(|group| !group.is_empty());

    merged
}
