//! The row limit of the scan.

use std::sync::atomic::{AtomicUsize, Ordering};

/// The row limit of one partition and the rows whose read is planned.
#[derive(Debug)]
pub(crate) struct RowBudget {
    /// The row limit. `None` for no limit.
    limit: Option<usize>,
    /// The rows of the planned reads.
    planned: AtomicUsize,
}

impl RowBudget {
    /// A budget of `limit` rows with no planned read.
    pub(crate) fn new(limit: Option<usize>) -> Self {
        Self {
            limit,
            planned: AtomicUsize::new(0),
        }
    }

    /// True when the planned reads hold the rows of the limit.
    pub(crate) fn reached(&self) -> bool {
        self.limit
            .is_some_and(|limit| self.planned.load(Ordering::Relaxed) >= limit)
    }

    /// Add the `rows` of the next read to the planned rows. False when the limit is reached.
    pub(crate) fn take(&self, rows: usize) -> bool {
        let Some(limit) = self.limit else {
            return true;
        };
        // The check and the add are one step, so two planners cannot both pass the limit.
        self.planned
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |count| {
                (count < limit).then(|| count.saturating_add(rows))
            })
            .is_ok()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::RowBudget;

    #[test]
    fn no_limit_takes_each_read() {
        let budget = RowBudget::new(None);
        assert!(budget.take(usize::MAX));
        assert!(budget.take(usize::MAX));
        assert!(!budget.reached());
    }

    #[test]
    fn a_limit_takes_reads_until_it_has_its_rows() {
        let budget = RowBudget::new(Some(13));
        // The second read passes the limit, and it is the last one.
        assert!(budget.take(12));
        assert!(!budget.reached());
        assert!(budget.take(12));
        assert!(budget.reached());
        assert!(!budget.take(12));
    }

    #[test]
    fn a_limit_of_0_takes_no_read() {
        let budget = RowBudget::new(Some(0));
        assert!(budget.reached());
        assert!(!budget.take(1));
    }

    #[test]
    fn a_huge_read_does_not_overflow() {
        let budget = RowBudget::new(Some(usize::MAX));
        assert!(budget.take(usize::MAX - 1));
        // The sum of the two reads stays at the maximum.
        assert!(budget.take(usize::MAX));
        assert!(budget.reached());
        assert!(!budget.take(1));
    }

    #[test]
    fn threads_that_take_at_the_same_time_do_not_pass_the_limit() {
        // 10 reads of 10 rows hold the limit of 100 rows. Each other read must fail.
        let budget = Arc::new(RowBudget::new(Some(100)));
        let taken = Arc::new(AtomicUsize::new(0));
        let threads: Vec<_> = (0..8)
            .map(|_| {
                let (budget, taken) = (Arc::clone(&budget), Arc::clone(&taken));
                std::thread::spawn(move || {
                    for _ in 0..100 {
                        if budget.take(10) {
                            taken.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                })
            })
            .collect();
        for thread in threads {
            thread.join().unwrap();
        }
        assert_eq!(taken.load(Ordering::Relaxed), 10);
        assert!(budget.reached());
    }
}
