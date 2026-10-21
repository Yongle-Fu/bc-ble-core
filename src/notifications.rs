//! Keep synchronous consumers off the asynchronous notification intake task.

use btleplug::api::ValueNotification;
use futures::{Stream, StreamExt};
use std::{
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Instant,
};
use tokio::sync::mpsc;

pub(crate) type NotificationStream = Pin<Box<dyn Stream<Item = ValueNotification> + Send>>;
pub(crate) const RECEIVE_QUEUE_CAPACITY: usize = 256;
type QueuedNotification = (Instant, ValueNotification);

pub(crate) struct NotificationWorker {
    pub(crate) sender: mpsc::Sender<QueuedNotification>,
    pub(crate) cancelled: Arc<AtomicBool>,
    task: tokio::task::JoinHandle<()>,
}

impl NotificationWorker {
    pub(crate) fn start(
        id: String,
        capacity: usize,
        mut deliver: impl FnMut(ValueNotification) + Send + 'static,
    ) -> Self {
        let (sender, mut receiver) = mpsc::channel::<QueuedNotification>(capacity);
        let cancelled = Arc::new(AtomicBool::new(false));
        let stopped = cancelled.clone();
        let task = tokio::task::spawn_blocking(move || {
            let mut processed = 0_u64;
            let mut max_queue_delay_us = 0_u128;
            while let Some((queued_at, value)) = receiver.blocking_recv() {
                if stopped.load(Ordering::Acquire) {
                    break;
                }
                max_queue_delay_us = max_queue_delay_us.max(queued_at.elapsed().as_micros());
                deliver(value);
                processed += 1;
            }
            log::debug!(
                "Notification worker stopped: device={id} processed={processed} max_queue_delay_us={max_queue_delay_us}"
            );
        });
        Self {
            sender,
            cancelled,
            task,
        }
    }

    pub(crate) async fn stop(self) {
        self.cancelled.store(true, Ordering::Release);
        drop(self.sender);
        // A running callback cannot be aborted. Join it before allowing a new
        // connection to reuse parser state; queued old notifications are discarded.
        if let Err(error) = self.task.await {
            log::error!("Notification worker failed: {error}");
        }
    }
}

pub(crate) async fn receive_notifications(
    mut values: NotificationStream,
    sender: &mpsc::Sender<QueuedNotification>,
    cancelled: &AtomicBool,
) -> Result<(), &'static str> {
    while let Some(value) = values.next().await {
        // Do not wait for a slow consumer while the backend's smaller broadcast
        // buffer can overflow. End this stream explicitly instead of skipping data.
        if let Err(error) = sender.try_send((Instant::now(), value)) {
            cancelled.store(true, Ordering::Release);
            return Err(match error {
                mpsc::error::TrySendError::Full(_) => "receive queue full",
                mpsc::error::TrySendError::Closed(_) => "notification worker stopped",
            });
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn notification(sequence: u8) -> ValueNotification {
        ValueNotification {
            uuid: Default::default(),
            service_uuid: Default::default(),
            value: vec![sequence],
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn blocked_consumer_does_not_block_intake_or_runtime_and_preserves_order() {
        let (entered, started) = tokio::sync::oneshot::channel();
        let mut entered = Some(entered);
        let (release, wait) = std::sync::mpsc::channel();
        let (delivered, mut received) = mpsc::unbounded_channel();
        let worker = NotificationWorker::start(
            "offline-order".into(),
            RECEIVE_QUEUE_CAPACITY,
            move |value| {
                if let Some(entered) = entered.take() {
                    entered.send(()).unwrap();
                    // Bound the wait even if an assertion on the async side fails.
                    let _ = wait.recv_timeout(Duration::from_secs(1));
                }
                delivered.send(value.value[0]).unwrap();
            },
        );
        worker
            .sender
            .try_send((Instant::now(), notification(0)))
            .unwrap();
        started.await.unwrap();
        let stream = Box::pin(futures::stream::iter((1..=32).map(notification)));
        tokio::time::timeout(Duration::from_millis(200), async {
            receive_notifications(stream, &worker.sender, &worker.cancelled)
                .await
                .unwrap();
            tokio::task::yield_now().await;
        })
        .await
        .expect("A synchronous callback must not block the runtime or intake");
        release.send(()).unwrap();
        for expected in 0..=32 {
            assert_eq!(
                tokio::time::timeout(Duration::from_secs(1), received.recv())
                    .await
                    .unwrap(),
                Some(expected)
            );
        }
        worker.stop().await;
    }

    #[tokio::test]
    async fn full_or_closed_queue_is_an_explicit_stream_failure() {
        let (sender, receiver) = mpsc::channel(2);
        let cancelled = AtomicBool::new(false);
        let stream = Box::pin(futures::stream::iter((0..3).map(notification)));
        assert_eq!(
            receive_notifications(stream, &sender, &cancelled).await,
            Err("receive queue full")
        );
        assert!(cancelled.load(Ordering::Acquire));
        drop(receiver);
        let stream = Box::pin(futures::stream::iter([notification(4)]));
        assert_eq!(
            receive_notifications(stream, &sender, &cancelled).await,
            Err("notification worker stopped")
        );
    }

    #[tokio::test]
    async fn overflow_discards_queued_notifications_after_the_active_callback() {
        let (entered, started) = tokio::sync::oneshot::channel();
        let mut entered = Some(entered);
        let (release, wait) = std::sync::mpsc::channel();
        let (delivered, mut received) = mpsc::unbounded_channel();
        let worker = NotificationWorker::start("offline-overflow".into(), 2, move |value| {
            if let Some(entered) = entered.take() {
                entered.send(()).unwrap();
                let _ = wait.recv_timeout(Duration::from_secs(1));
            }
            delivered.send(value.value[0]).unwrap();
        });
        worker
            .sender
            .try_send((Instant::now(), notification(0)))
            .unwrap();
        started.await.unwrap();
        let stream = Box::pin(futures::stream::iter((1..=3).map(notification)));
        assert_eq!(
            receive_notifications(stream, &worker.sender, &worker.cancelled).await,
            Err("receive queue full")
        );
        release.send(()).unwrap();
        worker.stop().await;
        assert_eq!(received.recv().await, Some(0));
        assert_eq!(received.recv().await, None);
    }

    #[tokio::test]
    async fn stop_joins_the_active_callback_and_discards_queued_old_notifications() {
        let (entered, started) = tokio::sync::oneshot::channel();
        let mut entered = Some(entered);
        let (release, wait) = std::sync::mpsc::channel();
        let (delivered, mut received) = mpsc::unbounded_channel();
        let worker = NotificationWorker::start("offline-stop".into(), 2, move |value| {
            if let Some(entered) = entered.take() {
                entered.send(()).unwrap();
                let _ = wait.recv_timeout(Duration::from_secs(1));
            }
            delivered.send(value.value[0]).unwrap();
        });
        worker
            .sender
            .try_send((Instant::now(), notification(1)))
            .unwrap();
        started.await.unwrap();
        worker
            .sender
            .try_send((Instant::now(), notification(2)))
            .unwrap();
        let mut stop = Box::pin(worker.stop());
        assert!(futures::poll!(stop.as_mut()).is_pending());
        release.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(1), stop)
            .await
            .unwrap();
        assert_eq!(received.recv().await, Some(1));
        assert_eq!(received.recv().await, None);
    }
}
