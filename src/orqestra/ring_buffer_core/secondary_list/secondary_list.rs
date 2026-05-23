use std::{
    ptr::null_mut,
    sync::atomic::{AtomicPtr, Ordering},
};

use crate::{ExecutableTask, OrqestraJobTrait, OrqestraTaskTrait};

pub(crate) struct SecondaryList<T, J, O>
where
    T: OrqestraTaskTrait<O> + 'static,
    J: OrqestraJobTrait<O> + 'static,
    O: 'static,
{
    dummy: AtomicPtr<ExecutableTask<T, J, O>>,
    head: AtomicPtr<ExecutableTask<T, J, O>>,
    tail: AtomicPtr<ExecutableTask<T, J, O>>,
}

impl<T, J, O> SecondaryList<T, J, O>
where
    T: OrqestraTaskTrait<O> + 'static,
    J: OrqestraJobTrait<O> + 'static,
    O: 'static,
{
    pub(crate) fn new() -> SecondaryList<T, J, O> {
        let dummy_ptr = Box::into_raw(Box::new(ExecutableTask::new_dummy()));
        Self {
            dummy: AtomicPtr::new(dummy_ptr),
            head: AtomicPtr::new(dummy_ptr),
            tail: AtomicPtr::new(dummy_ptr),
        }
    }

    pub(crate) fn push_front(&self, task: ExecutableTask<T, J, O>) {
        let new_head = Box::into_raw(Box::new(task));
        let prev_head = self.head.swap(new_head, Ordering::Relaxed);

        unsafe { (*prev_head).next().store(new_head, Ordering::Relaxed) };
    }

    pub(crate) fn pop_back(&self) -> Option<Box<ExecutableTask<T, J, O>>> {
        unsafe {
            let dummy = self.tail.load(Ordering::Relaxed);
            let executable_task_ptr = (*dummy).next().load(Ordering::Relaxed);
            if executable_task_ptr.is_null() {
                return None;
            }

            let candidate_task_ptr = (*executable_task_ptr).next().load(Ordering::Relaxed);

            let executable_ptr = match (*dummy).next().compare_exchange(
                executable_task_ptr,
                candidate_task_ptr,
                Ordering::Release,
                Ordering::Acquire,
            ) {
                Err(_) => return None,
                Ok(executable) => executable,
            };

            Some(Box::from_raw(executable_ptr))
        }
    }
}
