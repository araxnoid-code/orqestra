use std::{cell::RefCell, sync::Arc};

/// trait `OrqestraTaskTrait` memungkinkan suatu type data yang mengimplementasikannya
/// dapat menjadi task yang dapat dijalankan oleh orqestra
pub trait OrqestraTaskTrait {}

/// trait `OrqestraJobTrait` memungkinkan suatu type data yang mengimplementasikannya
/// dapat menjadi job yang dapat dijalankan oleh orqestra
pub trait OrqestraJobTrait {}

/// WaitingTask adalah type data yang digunakan saat spawn suatu task.
/// WaitingTask tidak hanya sebuah task independent tanpa adanya kemampuan penjadwalan.
pub struct WaitingTask<T, O>
where
    T: OrqestraTaskTrait,
{
    f: T,
    return_value: Arc<RefCell<Option<O>>>,
}

impl<T, O> WaitingTask<T, O>
where
    T: OrqestraTaskTrait,
{
    /// membuat WaitingTask
    pub fn new(f: T) -> WaitingTask<T, O> {
        Self {
            f,
            return_value: Arc::new(RefCell::new(None)),
        }
    }
}

/// ExecutableTask berfungsi untuk menghimpan Task dan Job pada satu type data enum.
/// berfungsi untuk Orqestra agar bisa mengeksekusi Task dan Job secara bersamaan
pub(crate) enum ExecutableTask<T, O>
where
    T: OrqestraTaskTrait,
{
    Task(WaitingTask<T, O>),
}
