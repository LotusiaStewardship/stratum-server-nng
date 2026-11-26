//! Cancel-safe reception of lotusd NNG pub/sub events.
//!
//! `PubInterface::recv_async()` is not cancel-safe: every call starts an nng
//! receive on a fresh `Aio` and the received message is only handed over
//! through a channel owned by the returned future. When that future is
//! dropped by `tokio::select!` (another branch won) while the receive has
//! already completed, the message is dropped with it. The event consumer
//! re-creates that future on every loop iteration, so pub/sub events could
//! be lost, and a lost `miningwrkchg` leaves miners on a stale job until the
//! next work change.
//!
//! Here a dedicated OS thread owns the SUB socket, receives with a short
//! timeout (so it notices `stop()`), parses messages with bitcoinsuite's
//! flatbuffer types and forwards them over a bounded tokio channel, whose
//! `recv()` is cancel-safe.
//!
//! The listener also reports every (re)connection of the SUB socket to
//! lotusd as [`NodeEvent::Connected`], from an nng pipe notification: after
//! lotusd restarts, nng redials on its own but no `miningwrkchg` may arrive
//! for a while, so the consumer must refresh the job itself.

use anyhow::Result;
use bitcoinsuite_bitcoind_nng::nng_interface_generated::nng_interface as fbs;
use bitcoinsuite_bitcoind_nng::{BlockConnected, BlockDisconnected, MiningWorkChanged};
use nng::options::protocol::pubsub::Subscribe;
use nng::options::{Options, RecvBufferSize, RecvTimeout};
use nng::{PipeEvent, Protocol, Socket};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;

/// Pub/sub topics the event consumer needs.
pub const TOPICS: [&str; 3] = ["miningwrkchg", "blkconnected", "blkdisconctd"];

/// Length of the topic prefix of every lotusd pub message.
const PREFIX_LEN: usize = 12;

/// How often the receive thread checks for `stop()`.
const RECV_TIMEOUT: Duration = Duration::from_millis(500);

/// An event received from lotusd's pub socket.
#[derive(Debug, Clone)]
pub enum NodeEvent {
    MiningWorkChanged(MiningWorkChanged),
    BlockConnected(BlockConnected),
    BlockDisconnected(BlockDisconnected),
    /// A connection (pipe) to lotusd was established, the first one or a
    /// reconnection.
    Connected,
    /// The connection (pipe) to lotusd was lost; nng redials on its own.
    Disconnected,
}

/// Parse one pub message (12-byte topic prefix + flatbuffer payload).
/// Returns `None` for an unknown topic or an invalid payload.
pub fn parse_message(msg: &[u8]) -> Option<NodeEvent> {
    if msg.len() < PREFIX_LEN {
        return None;
    }
    let (prefix, payload) = msg.split_at(PREFIX_LEN);
    let opts = flatbuffers::VerifierOptions {
        max_tables: 0xffff_ffff,
        ..Default::default()
    };
    match prefix {
        b"miningwrkchg" => {
            let m = flatbuffers::root_with_opts::<fbs::MiningWorkChanged>(&opts, payload).ok()?;
            MiningWorkChanged::from_fbs(m)
                .ok()
                .map(NodeEvent::MiningWorkChanged)
        }
        b"blkconnected" => {
            let m = flatbuffers::root_with_opts::<fbs::BlockConnected>(&opts, payload).ok()?;
            BlockConnected::from_fbs(m)
                .ok()
                .map(NodeEvent::BlockConnected)
        }
        b"blkdisconctd" => {
            let m = flatbuffers::root_with_opts::<fbs::BlockDisconnected>(&opts, payload).ok()?;
            BlockDisconnected::from_fbs(m)
                .ok()
                .map(NodeEvent::BlockDisconnected)
        }
        _ => None,
    }
}

/// Owns the SUB socket and the thread that reads it.
pub struct PubListener {
    stop: Arc<AtomicBool>,
    handle: Option<std::thread::JoinHandle<()>>,
}

impl PubListener {
    /// Dial `url`, subscribe to [`TOPICS`] and forward every parsed event,
    /// and every connection change, to `tx` from a dedicated thread.
    pub fn spawn(url: &str, tx: mpsc::Sender<NodeEvent>) -> Result<Self> {
        let sock = Socket::new(Protocol::Sub0)?;
        // Same buffer as PubInterface: absorbs bursts during reorgs.
        sock.set_opt::<RecvBufferSize>(1024)?;
        sock.set_opt::<RecvTimeout>(Some(RECV_TIMEOUT))?;
        for topic in TOPICS {
            sock.set_opt::<Subscribe>(topic.as_bytes().to_vec())?;
        }
        let pipe_tx = tx.clone();
        sock.pipe_notify(move |_pipe, event| {
            let event = match event {
                PipeEvent::AddPost => NodeEvent::Connected,
                PipeEvent::RemovePost => NodeEvent::Disconnected,
                _ => return,
            };
            // Runs on an nng thread: must neither block nor panic.
            let _ = pipe_tx.try_send(event);
        })?;
        sock.dial(url)?;

        let stop = Arc::new(AtomicBool::new(false));
        let thread_stop = stop.clone();
        let handle = std::thread::Builder::new()
            .name("nng-pub-listener".into())
            .spawn(move || {
                while !thread_stop.load(Ordering::Relaxed) {
                    match sock.recv() {
                        Ok(msg) => {
                            if let Some(event) = parse_message(&msg[..]) {
                                if tx.blocking_send(event).is_err() {
                                    break; // consumer gone
                                }
                            }
                        }
                        Err(nng::Error::TimedOut) => {}
                        Err(nng::Error::Closed) => break,
                        Err(_) => std::thread::sleep(Duration::from_millis(100)),
                    }
                }
            })?;
        Ok(Self {
            stop,
            handle: Some(handle),
        })
    }

    /// Stop the receive thread and wait for it (at most one receive timeout).
    /// Close or drop the channel's receiver first, so that a send blocked on
    /// a full channel returns.
    pub fn stop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

impl Drop for PubListener {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mining_work_changed(epoch: u64) -> Vec<u8> {
        let mut fbb = flatbuffers::FlatBufferBuilder::new();
        let hash = fbs::Hash::new(&[3u8; 32]);
        let block_hash =
            fbs::BlockHash::create(&mut fbb, &fbs::BlockHashArgs { hash: Some(&hash) });
        let msg = fbs::MiningWorkChanged::create(
            &mut fbb,
            &fbs::MiningWorkChangedArgs {
                reason: fbs::MiningWorkChangeReason::NEW_TIP,
                block_hash: Some(block_hash),
                height: 10,
                node_time: 1,
                template_epoch: epoch,
            },
        );
        fbb.finish(msg, None);
        let mut out = b"miningwrkchg".to_vec();
        out.extend_from_slice(fbb.finished_data());
        out
    }

    fn ipc_url(dir: &tempfile::TempDir) -> String {
        format!("ipc://{}/pub.pipe", dir.path().display())
    }

    /// A publisher whose per-subscriber queue holds a whole test burst: PUB
    /// drops messages for a subscriber whose queue is full (16 by default),
    /// which would make the tests measure nng's flow control, not ours.
    fn publisher(url: &str) -> Socket {
        let pubs = Socket::new(Protocol::Pub0).unwrap();
        pubs.set_opt::<nng::options::SendBufferSize>(1024).unwrap();
        pubs.listen(url).unwrap();
        pubs
    }

    /// Publish `probe` until the subscriber receives one event: the
    /// subscription has then reached the publisher.
    async fn wait_until_subscribed(pubs: &Socket, rx: &mut mpsc::Receiver<NodeEvent>) {
        for _ in 0..200 {
            pubs.send(nng::Message::from(&mining_work_changed(u64::MAX)[..]))
                .unwrap();
            if let Ok(Some(NodeEvent::MiningWorkChanged(m))) =
                tokio::time::timeout(Duration::from_millis(50), rx.recv()).await
            {
                if m.template_epoch == u64::MAX {
                    // Drain the probes still in flight.
                    while let Ok(Some(_)) =
                        tokio::time::timeout(Duration::from_millis(100), rx.recv()).await
                    {
                    }
                    return;
                }
            }
        }
        panic!("subscriber never received a probe");
    }

    async fn wait_for_connected(rx: &mut mpsc::Receiver<NodeEvent>) {
        loop {
            match tokio::time::timeout(Duration::from_secs(10), rx.recv())
                .await
                .expect("no Connected event")
            {
                Some(NodeEvent::Connected) => return,
                Some(_) => {}
                None => panic!("listener channel closed"),
            }
        }
    }

    /// A publisher restart (lotusd restart) is reported as Disconnected then
    /// Connected, and events flow again afterwards.
    #[tokio::test]
    async fn test_reports_reconnection_after_publisher_restart() {
        let dir = tempfile::tempdir().unwrap();
        let url = ipc_url(&dir);
        let pubs = publisher(&url);
        let (tx, mut rx) = mpsc::channel(1024);
        let mut listener = PubListener::spawn(&url, tx).unwrap();
        wait_for_connected(&mut rx).await;
        wait_until_subscribed(&pubs, &mut rx).await;

        // Restart the publisher.
        pubs.close();
        loop {
            match tokio::time::timeout(Duration::from_secs(10), rx.recv())
                .await
                .expect("no Disconnected event")
            {
                Some(NodeEvent::Disconnected) => break,
                Some(_) => {}
                None => panic!("listener channel closed"),
            }
        }
        let pubs = publisher(&url);
        wait_for_connected(&mut rx).await;
        wait_until_subscribed(&pubs, &mut rx).await;

        pubs.send(nng::Message::from(&mining_work_changed(7)[..]))
            .unwrap();
        match tokio::time::timeout(Duration::from_secs(5), rx.recv()).await {
            Ok(Some(NodeEvent::MiningWorkChanged(m))) => assert_eq!(m.template_epoch, 7),
            other => panic!("unexpected {other:?}"),
        }
        drop(rx);
        listener.stop();
    }

    #[test]
    fn test_parse_message_mining_work_changed() {
        match parse_message(&mining_work_changed(42)) {
            Some(NodeEvent::MiningWorkChanged(m)) => {
                assert_eq!(m.template_epoch, 42);
                assert_eq!(m.height, 10);
            }
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn test_parse_message_ignores_invalid_input() {
        assert!(parse_message(b"short").is_none());
        assert!(parse_message(b"unknowntopic\x00\x00\x00\x00").is_none());
        assert!(parse_message(b"miningwrkchg\x01\x02").is_none());
    }

    /// The stall scenario: the consumer's `select!` has a competing branch
    /// that is ready very often. No event may be lost or reordered.
    #[tokio::test]
    async fn test_no_event_lost_with_competing_select_branch() {
        let dir = tempfile::tempdir().unwrap();
        let url = ipc_url(&dir);
        let pubs = publisher(&url);
        let (tx, mut rx) = mpsc::channel(1024);
        let mut listener = PubListener::spawn(&url, tx).unwrap();
        wait_until_subscribed(&pubs, &mut rx).await;

        let n = 300u64;
        for i in 0..n {
            pubs.send(nng::Message::from(&mining_work_changed(i)[..]))
                .unwrap();
        }
        let mut seen = 0u64;
        let (busy_tx, mut busy_rx) = mpsc::unbounded_channel::<()>();
        for _ in 0..100_000 {
            busy_tx.send(()).unwrap();
        }
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        while seen < n && tokio::time::Instant::now() < deadline {
            tokio::select! {
                ev = rx.recv() => {
                    if let Some(NodeEvent::MiningWorkChanged(m)) = ev {
                        assert_eq!(m.template_epoch, seen);
                        seen += 1;
                    }
                }
                _ = busy_rx.recv() => {}
            }
        }
        assert_eq!(seen, n);
        drop(rx);
        listener.stop();
    }
}
