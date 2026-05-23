use std::sync::atomic::AtomicPtr;

use crate::{ExecutableTask, OrqestraJobTrait, OrqestraTaskTrait};

pub(crate) struct SecondaryList<T, J, O>
where
    T: OrqestraTaskTrait<O> + 'static,
    J: OrqestraJobTrait<O> + 'static,
    O: 'static,
{
    head: AtomicPtr<ExecutableTask<T, J, O>>,
    tail: AtomicPtr<ExecutableTask<T, J, O>>,
}
