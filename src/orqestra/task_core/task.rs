use std::{cell::RefCell, sync::Arc};

/// trait `OrqestraTaskTrait` memungkinkan suatu type data yang mengimplementasikannya
/// dapat menjadi task yang dapat dijalankan oleh orqestra
pub trait OrqestraTaskTrait {}

/// trait `OrqestraJobTrait` memungkinkan suatu type data yang mengimplementasikannya
/// dapat menjadi job yang dapat dijalankan oleh orqestra
pub trait OrqestraJobTrait {}

/// trait `OrqestraOutTrait` memungkinkan suatu type data yang mengimplementasikannya
/// dapat menjadi Output yang dapat dijalankan oleh orqestra
// trait OrqestraOutTrait {}

///
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
    pub fn new(f: T) -> WaitingTask<T, O> {
        Self {
            f,
            return_value: Arc::new(RefCell::new(None)),
        }
    }
}

///
pub(crate) enum ExecutableTask<T, O>
where
    T: OrqestraTaskTrait,
{
    Task(WaitingTask<T, O>),
}
