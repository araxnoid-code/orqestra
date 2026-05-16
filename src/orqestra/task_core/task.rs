use std::{
    cell::RefCell,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

/// trait `OrqestraTaskTrait` memungkinkan suatu type data yang mengimplementasikannya
/// dapat menjadi task yang dapat dijalankan oleh orqestra
pub trait OrqestraTaskTrait<O>
where
    O: 'static,
{
    fn execute(&self) -> O;
}

/// trait `OrqestraJobTrait` memungkinkan suatu type data yang mengimplementasikannya
/// dapat menjadi job yang dapat dijalankan oleh orqestra
pub trait OrqestraJobTrait {}

/// WaitingTask adalah type data yang digunakan saat spawn suatu task.
/// WaitingTask tidak hanya sebuah task independent tanpa adanya kemampuan penjadwalan.
pub struct WaitingTask<T, O>
where
    T: OrqestraTaskTrait<O>,
    O: 'static,
{
    f: T,
    return_value: Arc<(RefCell<Option<O>>, AtomicBool)>,
}

impl<T, O> WaitingTask<T, O>
where
    T: OrqestraTaskTrait<O>,
    O: 'static,
{
    /// membuat WaitingTask
    pub fn new(f: T) -> WaitingTask<T, O> {
        Self {
            f,
            return_value: Arc::new((RefCell::new(None), AtomicBool::new(false))),
        }
    }
}

/// ExecutableTask berfungsi untuk menghimpan Task dan Job pada satu type data enum.
/// berfungsi untuk Orqestra agar bisa mengeksekusi Task dan Job secara bersamaan
pub(crate) enum ExecutableTask<T, O>
where
    T: OrqestraTaskTrait<O>,
    O: 'static,
{
    Task(WaitingTask<T, O>),
}

impl<T, O> ExecutableTask<T, O>
where
    T: OrqestraTaskTrait<O> + 'static,
    O: 'static,
{
    pub(crate) fn execute(&self) -> O {
        match self {
            ExecutableTask::Task(task) => task.f.execute(),
        }
    }

    pub(crate) fn update_value(&self, value: O) {
        match self {
            ExecutableTask::Task(task) => {
                task.return_value.0.replace(Some(value));
                task.return_value.1.store(true, Ordering::Relaxed);
            }
        };
    }

    pub(crate) fn execute_then_update(&self) {
        self.update_value(self.execute());
    }
}
