use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::Arc;

/// Tracks the latest known chain tip height from NNG pub/sub events.
///
/// Shared between the NNG event consumer and the maturation checker.
/// The consumer updates this on each `BlockConnected` / `BlockDisconnected`,
/// and the payout system reads it to compute confirmation depths.
///
/// ## Semantics
///
/// - `BlockConnected`: tip advances monotonically (`max`).
/// - `BlockDisconnected`: tip decrements by 1 only when the disconnected
///   block **is** the tip. Disconnections below the tip do not affect it.
///
/// Initial value of 0 is harmless — before the first template refresh
/// no blocks exist, so no immature blocks are queried.
#[derive(Clone, Debug)]
pub struct ChainTip(Arc<AtomicI32>);

impl ChainTip {
    /// Create a new chain tip tracker starting at `initial`.
    pub fn new(initial: i32) -> Self {
        Self(Arc::new(AtomicI32::new(initial)))
    }

    /// Update on a `BlockConnected` event.
    ///
    /// The tip can only advance — if `height` is lower than the current
    /// value (e.g. stale event), it is ignored.
    pub fn update_block_connected(&self, height: i32) {
        let prev = self.0.load(Ordering::Relaxed);
        if height > prev {
            self.0.store(height, Ordering::Relaxed);
        }
    }

    /// Update on a `BlockDisconnected` event.
    ///
    /// Only decrements when the disconnected block's height matches the
    /// current tip. Disconnections of older blocks (e.g. deep reorg) are
    /// ignored — those are handled by the orphaning logic in
    /// `handle_block_disconnected`.
    pub fn update_block_disconnected(&self, height: i32) {
        let prev = self.0.load(Ordering::Relaxed);
        if height == prev && prev > 0 {
            self.0.store(prev - 1, Ordering::Relaxed);
        }
    }

    /// Get the current chain tip height.
    pub fn get(&self) -> i32 {
        self.0.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_starts_at_zero() {
        let tip = ChainTip::new(0);
        assert_eq!(tip.get(), 0);
    }

    #[test]
    fn test_connected_advances_tip() {
        let tip = ChainTip::new(0);
        tip.update_block_connected(10);
        assert_eq!(tip.get(), 10);
    }

    #[test]
    fn test_connected_does_not_reverse() {
        let tip = ChainTip::new(0);
        tip.update_block_connected(10);
        tip.update_block_connected(5); // lower height, should be ignored
        assert_eq!(tip.get(), 10);
    }

    #[test]
    fn test_disconnected_at_tip_decrements() {
        let tip = ChainTip::new(0);
        tip.update_block_connected(10);
        tip.update_block_disconnected(10);
        assert_eq!(tip.get(), 9);
    }

    #[test]
    fn test_disconnected_below_tip_noop() {
        let tip = ChainTip::new(0);
        tip.update_block_connected(10);
        tip.update_block_disconnected(5); // below tip, should be ignored
        assert_eq!(tip.get(), 10);
    }

    #[test]
    fn test_disconnected_above_tip_noop() {
        let tip = ChainTip::new(0);
        tip.update_block_connected(10);
        tip.update_block_disconnected(15); // above tip, should be ignored
        assert_eq!(tip.get(), 10);
    }

    #[test]
    fn test_disconnected_at_zero_is_noop() {
        let tip = ChainTip::new(0);
        tip.update_block_disconnected(0); // would underflow, should be noop
        assert_eq!(tip.get(), 0);
    }

    #[test]
    fn test_chain_tip_is_send_sync() {
        fn assert_send<T: Send>() {}
        fn assert_sync<T: Sync>() {}
        assert_send::<ChainTip>();
        assert_sync::<ChainTip>();
    }
}
