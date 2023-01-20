use crate::Error;
use std::future::Future;
use std::marker::Send;
use std::pin::Pin;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::{Mutex, TryLockError, Arc, Condvar};
use std::task::{Context, Poll};
use std::thread::{self, available_parallelism};
use std::time::Duration;

pub(crate) type BoxFuture<'a, T> = Box<dyn Future<Output = T> + Send + Sync + 'a>;

pub(crate) fn execute_a_bunch<'a, I>(tasks: I) -> Result<(), Error>
    where I: IntoIterator<Item = BoxFuture<'a, Result<(), Error>>>,
    I::IntoIter: DoubleEndedIterator,
{
    let earliest_woken = Arc::new(AtomicUsize::new(usize::MAX));
    let condvar = Arc::new(Condvar::new());
    let tasks: Vec<_> = tasks.into_iter().rev()
        .enumerate() // after rev
        .map(|(current_future_index, fut)| {
            let e = earliest_woken.clone();
            let c = condvar.clone();
            let waker = waker_fn::waker_fn(move || {
                e.fetch_min(current_future_index, Relaxed); // re-poll
                c.notify_one(); // break wait of some thread
            });
            Mutex::new(Some((Pin::from(fut), waker)))
        }).collect();
    let err = Mutex::new(None);
    let waiter = Mutex::new(());
    thread::scope(|s| {
        let worker = || {
            'outer: loop {
                let mut finished_tasks = 0;
                let mut busy_tasks = 0;
                for (current_task, t) in tasks.iter().enumerate() {
                    match t.try_lock() {
                        Ok(mut t) => if let Some((fut, waker)) = t.as_mut() {
                            let mut ctx = Context::from_waker(&waker);
                            match Future::poll(fut.as_mut(), &mut ctx) {
                                Poll::Pending => {
                                    if earliest_woken.load(Relaxed) < current_task {
                                        // intentional race to avoid frequent writes,
                                        // harmless, because this thread will poll anyway even if other wakers ran now
                                        earliest_woken.store(usize::MAX, Relaxed);
                                        continue 'outer;
                                    }
                                },
                                Poll::Ready(res) => {
                                    *t = None; // ensure it won't be polled again
                                    condvar.notify_all(); // this may have been the last job, so make other threads check for exit condition
                                    if let Err(e) = res {
                                        // first to fail sets the result. TODO combine errors smartly, maybe disconnected channels err too soon
                                        err.lock().unwrap().get_or_insert(e);
                                        return;
                                    }
                                    continue;
                                },
                            }
                        } else {
                            finished_tasks += 1;
                        },
                        Err(TryLockError::WouldBlock) => {
                            busy_tasks += 1;
                            continue
                        },
                        Err(TryLockError::Poisoned(_)) => {
                            err.lock().unwrap().get_or_insert(Error::ThreadSend);
                            return;
                        },
                    };
                }
                if finished_tasks == tasks.len() {
                    break;
                }
                if earliest_woken.load(Relaxed) == usize::MAX {
                    let (_, timedout) = condvar.wait_timeout(waiter.lock().unwrap(), Duration::from_secs(5)).unwrap();
                    if timedout.timed_out() {
                        eprintln!("••• OOOOF from busy {busy_tasks} finished {finished_tasks} total {}", tasks.len());
                    }
                }
            }
        };

        let threads = available_parallelism().map(|t| t.get().max(2)).unwrap_or(8);
        for n in 0..threads {
            thread::Builder::new().name(format!("t{n}")).spawn_scoped(s, worker.clone()).unwrap();
        }
    });
    if let Ok(Some(e)) = err.into_inner() {
        Err(e)
    } else {
        Ok(())
    }
}

