use std::{
    hint::spin_loop,
    ops::Deref,
    ptr::null_mut,
    sync::atomic::{AtomicBool, AtomicPtr, AtomicU64, AtomicUsize, Ordering},
    thread::yield_now,
};

use crossbeam_queue::SegQueue;

use crate::{ExecutableTask, OrqestraJobTrait, OrqestraTaskTrait};

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

    /// registered
    pub registered_count: AtomicUsize,

    /// secondary_list
    pub(crate) secondary_list: SegQueue<ExecutableTask<T, J, O>>,
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

            registered_count: AtomicUsize::new(0),

            secondary_list: SegQueue::new(),
        }
    }

    /// put ExecutableTask into ring-buffer
    /// insert data based on the index obtained by the head in the ring buffer,
    /// synchronize with workers based on the index location and the empty property
    /// in the RingBufferSpace which is where the executable task is stored
    ///
    /// ## ring-buffer and secondary_list
    /// In storing executable tasks, there are 2 allocation methods:
    /// ### ring-buffer
    /// is the main storage in the form of a queue,
    /// allocation uses the ring-buffer concept via `head` and `tail` ring-buffer has a static size and can be full,
    /// the status of the ring-buffer being full or not is based on the `registered_count` property
    /// which will count the executables allocated using enqueue and deallocated using dequeue
    /// ### secondary_list
    /// When the ring buffer is full, the executable task will be allocated to the secondary_list.
    /// The secondary_list is dynamic and has no specific limitations in storing executable tasks
    /// other than the available memory size.
    /// *version/0.0.1 and so on*
    /// secondary_list usage in this version uses `crossbeam_queue::SegQueue`
    /// *warning*
    /// Because secondary_list is dynamic,
    /// it can cause memory problems if there are too many executable tasks.
    pub(crate) fn enqueue(&self, executable_task: ExecutableTask<T, J, O>) {
        if self.registered_count.fetch_add(1, Ordering::Release) >= RING_BUFFER_SIZE {
            self.registered_count.fetch_sub(1, Ordering::Release);
            self.secondary_push(executable_task);
            return;
        };

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
    /// ## ring-buffer only
    /// The allocation of executable tasks is only focused on the ring buffer,
    /// if the ring buffer is full it will give an Err.
    /// the status of the ring-buffer being full or not is based on the `registered_count` property
    /// which will count the executables allocated using enqueue and deallocated using dequeue
    pub(crate) fn try_enqueue(
        &self,
        executable_task: ExecutableTask<T, J, O>,
    ) -> Result<(), &'static str> {
        if self.registered_count.fetch_add(1, Ordering::Release) >= RING_BUFFER_SIZE {
            self.registered_count.fetch_sub(1, Ordering::Release);
            return Err("Cannot insert task because ring buffer is full");
        };

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

            Ok(())
        }
    }

    /// registering executable tasks in the ring-buffer without any problems due to the static size of the ring-buffer.
    /// ## there is empty space
    /// If you get an index that points to empty space,
    /// then that space will be immediately occupied to insert the executable task.
    /// ## there is no empty space
    /// If you get an index that points to a non-empty space,
    /// then the executable task that occupies that space will be swapped with the executable task you want to insert,
    /// the result of the swapped executable task will be the return value.
    pub(crate) fn enqueue_or_swap(
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
                self.registered_count.fetch_add(1, Ordering::Release);
                space.empty.store(false, Ordering::Relaxed);
                None
            }
        }
    }

    /// langsung memasukkan executable task ke dalam secondary_list
    pub(crate) fn secondary_push(&self, executable_task: ExecutableTask<T, J, O>) {
        self.in_task.fetch_add(1, Ordering::Relaxed);
        self.secondary_list.push(executable_task);
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
            self.registered_count.fetch_sub(1, Ordering::Release);
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
            self.registered_count.fetch_sub(1, Ordering::Release);
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
