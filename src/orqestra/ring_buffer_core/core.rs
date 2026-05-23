use std::{
    hint::spin_loop,
    ops::Deref,
    ptr::null_mut,
    sync::atomic::{AtomicBool, AtomicPtr, AtomicU64, Ordering},
    thread::yield_now,
};

use crate::{ExecutableTask, OrqestraJobTrait, OrqestraTaskTrait, SecondaryList};

/// counter, as a wrapper of AtomicU64.
#[repr(align(64))]
struct Counter {
    idx: AtomicU64,
}

impl Deref for Counter {
    type Target = AtomicU64;
    fn deref(&self) -> &Self::Target {
        &self.idx
    }
}

/// gives status to the process of taking tasks on the ring-buffer.
pub(crate) enum DequeueStatus<T, J, O>
where
    T: OrqestraTaskTrait<O> + 'static,
    J: OrqestraJobTrait<O> + 'static,
    O: 'static,
{
    /// when dequeuing to retrieve an Executable Task and succeeding at that time
    Ok(ExecutableTask<T, J, O>),

    /// When dequeuing to retrieve an ExecutableTask but getting no ExecutableTask,
    /// the Order will have the usize value as the index at the previously empty ExecutableTask location,
    /// useful for workers to periodically check the location at the index obtained until
    /// the main thread or other workers fill the index location with a new ExecutableTask.
    Order(usize),
}

/// Ring Buffer Space, functions as a space where Executable Tasks are stored.
#[repr(align(64))]
pub(crate) struct RingBufferSpace<T, J, O>
where
    T: OrqestraTaskTrait<O> + 'static,
    J: OrqestraJobTrait<O> + 'static,
    O: 'static,
{
    /// serves to store ExecutableTask
    task: Option<ExecutableTask<T, J, O>>,

    /// serves to indicate whether the space is empty
    empty: AtomicBool,
}
impl<T, J, O> RingBufferSpace<T, J, O>
where
    T: OrqestraTaskTrait<O> + 'static,
    J: OrqestraJobTrait<O> + 'static,
    O: 'static,
{
    /// RingBufferSpace initial with default value.
    fn new() -> RingBufferSpace<T, J, O> {
        Self {
            task: None,
            empty: AtomicBool::new(true),
        }
    }
}

/// RingBuffer, the core structure for building a ring buffer
/// that will hold every ExecutableTask created.
///
/// ## RING_BUFFER_SIZE
/// ring-buffer size depends on const value RING_BUFFER_SIZE,
/// manual initialization is required for RING_BUFFER_SIZE
pub struct RingBufferCore<T, J, O, const RING_BUFFER_SIZE: usize>
where
    T: OrqestraTaskTrait<O> + 'static,
    J: OrqestraJobTrait<O> + 'static,
    O: 'static,
{
    /// count each task that has been registered into the ring-buffer
    pub(crate) in_task: AtomicU64,

    /// functions to determine the index in allocating ExecutableTask in the ring buffer, the main function of the enqueue logic
    head: Counter,

    /// functions in determining the index in retrieving an ExecutableTask in the ring buffer, the main function of the dequeue logic
    tail: Counter,

    /// a place to store tasks that is possible in multi producer and multi consumer
    /// because of the synchronization between indexes by head and tail and by `RingBufferSpace`
    queue: AtomicPtr<Vec<RingBufferSpace<T, J, O>>>,

    /// secondary_list
    secondary_list: SecondaryList<T, J, O>,
}

impl<T, J, O, const RING_BUFFER_SIZE: usize> RingBufferCore<T, J, O, RING_BUFFER_SIZE>
where
    T: OrqestraTaskTrait<O> + 'static,
    J: OrqestraJobTrait<O> + 'static,
    O: 'static,
{
    /// RingBuffer initial
    pub(crate) fn new() -> RingBufferCore<T, J, O, RING_BUFFER_SIZE> {
        Self {
            in_task: AtomicU64::new(0),
            head: Counter {
                idx: AtomicU64::new(0),
            },

            tail: Counter {
                idx: AtomicU64::new(0),
            },

            queue: AtomicPtr::new(Box::into_raw(Box::new(
                (0..RING_BUFFER_SIZE)
                    .map(|_| RingBufferSpace::new())
                    .collect(),
            ))),

            secondary_list: SecondaryList::new(),
        }
    }

    /// put ExecutableTask into ring-buffer
    /// insert data based on the index obtained by the head in the ring buffer,
    /// synchronize with workers based on the index location and the empty property
    /// in the RingBufferSpace which is where the executable task is stored
    /// ## Blocking
    // When the ring buffer is full, blocking will occur
    // until there is space to allocate an ExecutableTask.
    pub(crate) fn enqueue(&self, executable_task: ExecutableTask<T, J, O>) {
        self.in_task.fetch_add(1, Ordering::Relaxed);

        let idx = self.head.fetch_add(1, Ordering::Relaxed) as usize & (RING_BUFFER_SIZE - 1);

        unsafe {
            let space = &mut (&mut (*self.queue.load(Ordering::Relaxed)))[idx];

            let mut yield_counter = 0;
            while !space.empty.load(Ordering::Relaxed) {
                spin_loop();
                if yield_counter >= 1000 {
                    yield_now();
                } else {
                    yield_counter += 1;
                }
            }

            space.task = Some(executable_task);
            space.empty.store(false, Ordering::Relaxed);
        }
    }

    /// put ExecutableTask into ring-buffer
    /// insert data based on the index obtained by the head in the ring buffer,
    /// synchronize with workers based on the index location and the empty property
    /// in the RingBufferSpace which is where the executable task is stored
    /// ## non-Blocking
    /// When the ring-buffer is full, it will return the data type Result::Err
    pub(crate) fn try_enqueue(
        &self,
        executable_task: ExecutableTask<T, J, O>,
    ) -> Result<(), &'static str> {
        self.in_task.fetch_add(1, Ordering::Relaxed);

        let idx = self.head.fetch_add(1, Ordering::Relaxed) as usize & (RING_BUFFER_SIZE - 1);

        unsafe {
            let space = &mut (&mut (*self.queue.load(Ordering::Relaxed)))[idx];
            if !space.empty.load(Ordering::Relaxed) {
                self.in_task.fetch_sub(1, Ordering::Relaxed);
                return Err("Cannot insert task because ring buffer is full");
            }

            space.task = Some(executable_task);
            space.empty.store(false, Ordering::Relaxed);
            Ok(())
        }
    }

    pub(crate) fn swap_enqueue(
        &self,
        executable_task: ExecutableTask<T, J, O>,
    ) -> Option<ExecutableTask<T, J, O>> {
        self.in_task.fetch_add(1, Ordering::Relaxed);
        let idx = self.head.fetch_add(1, Ordering::Relaxed) as usize & (RING_BUFFER_SIZE - 1);

        unsafe {
            let space = &mut (&mut (*self.queue.load(Ordering::Relaxed)))[idx];

            if !space.empty.load(Ordering::Relaxed) {
                Some(space.task.replace(executable_task).unwrap())
            } else {
                space.task = Some(executable_task);
                space.empty.store(false, Ordering::Relaxed);
                None
            }
        }
    }

    /// take ExecutableTask from ring buffer
    /// Retrieval is based on the index obtained via tail
    /// synchronization between workers based on the location index obtained via tail
    /// ## Reservation
    /// When the worker does not get an ExecutableTask at an index that it gets from the tail,
    /// the function will return DequeueStatus::Order(usize) which is useful for being stored by the worker
    /// and will be checked periodically until a thread adds an ExecutableTask to that index.
    pub(crate) fn dequeue(&self) -> DequeueStatus<T, J, O> {
        let idx = self.tail.fetch_add(1, Ordering::Relaxed) as usize & (RING_BUFFER_SIZE - 1);

        unsafe {
            let space = &mut (&mut (*self.queue.load(Ordering::Relaxed)))[idx];
            if space.empty.load(Ordering::Relaxed) {
                return DequeueStatus::Order(idx);
            }

            let executable_task = space.task.take().unwrap();
            space.empty.store(true, Ordering::Relaxed);

            return DequeueStatus::Ok(executable_task);
        }
    }

    /// take ExecutableTask from ring buffer
    /// Data retrieval is based on the index entered via the idx parameters.
    pub(crate) fn dequeue_via_order(&self, idx: usize) -> DequeueStatus<T, J, O> {
        unsafe {
            let space = &mut (&mut (*self.queue.load(Ordering::Relaxed)))[idx];
            if space.empty.load(Ordering::Relaxed) {
                return DequeueStatus::Order(idx);
            }

            let executable_task = space.task.take().unwrap();
            space.empty.store(true, Ordering::Relaxed);

            return DequeueStatus::Ok(executable_task);
        }
    }

    /// drop queue
    pub(crate) fn drop_queue(&self) {
        unsafe {
            drop(Box::from_raw(
                self.queue.swap(null_mut(), Ordering::Relaxed),
            ));
        }
    }
}
