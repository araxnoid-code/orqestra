use std::sync::{Arc, atomic::AtomicU64};

use crate::orqestra::task_core::{OrqestraTaskTrait, RingBuffer};

/// TaskCore, struktur utama dalam menajemen task yang di spawn
/// dan pengalokasiannya ke dalam ring-buffer
pub struct TaskCore<T, O, const RING_BUFFER_SIZE: usize>
where
    T: OrqestraTaskTrait + 'static,
    O: 'static,
{
    /// menghitung setiap task yang telah terdaftar ke dalam ring-buffer
    pub(crate) in_task: AtomicU64,

    /// ring-buffer tempat menyimpan ExecutableTask yang akan dieksekusi oleh workers
    pub(crate) ring_buffer: Arc<RingBuffer<T, O, RING_BUFFER_SIZE>>,
}

impl<T, O, const RING_BUFFER_SIZE: usize> TaskCore<T, O, RING_BUFFER_SIZE>
where
    T: OrqestraTaskTrait,
{
    /// initial TaskCore
    pub(crate) fn new() -> TaskCore<T, O, RING_BUFFER_SIZE> {
        Self {
            in_task: AtomicU64::new(0),
            ring_buffer: Arc::new(RingBuffer::new()),
        }
    }
}
