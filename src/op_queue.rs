#![allow(unsafe_code)]

use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::sync::atomic::{
    spin_loop_hint, AtomicPtr, AtomicU64, AtomicUsize, Ordering,
};

use crate::{debug_delay, Guard};

#[cfg(any(test, feature = "lock_free_delays"))]
const MAX_QUEUE_ITEMS: usize = 4;

#[cfg(not(any(test, feature = "lock_free_delays")))]
const MAX_QUEUE_ITEMS: usize = 64;

struct OpBlock<T> {
    len: AtomicUsize,
    block: UnsafeCell<[MaybeUninit<T>; MAX_QUEUE_ITEMS]>,
    next: AtomicPtr<OpBlock<T>>,
    complete: AtomicU64,
}

unsafe impl<T: Copy> Send for OpBlock<T> {}

impl<T> Default for OpBlock<T> {
    fn default() -> OpBlock<T> {
        OpBlock {
            len: AtomicUsize::new(0),
            block: UnsafeCell::new(unsafe {
                MaybeUninit::<[MaybeUninit<T>; MAX_QUEUE_ITEMS]>::uninit()
                    .assume_init()
            }),
            next: AtomicPtr::default(),
            complete: AtomicU64::new(0),
        }
    }
}

pub(crate) struct OpQueue<T: Copy> {
    writing: AtomicPtr<OpBlock<T>>,
    full_list: AtomicPtr<OpBlock<T>>,
}

impl<T: Copy> Default for OpQueue<T> {
    fn default() -> OpQueue<T> {
        OpQueue {
            writing: AtomicPtr::new(Box::into_raw(
                Box::new(OpBlock::default()),
            )),
            full_list: AtomicPtr::default(),
        }
    }
}

impl<T: Copy> OpQueue<T> {
    pub(crate) fn push(&self, item: T) -> bool {
        let mut filled = false;
        loop {
            debug_delay();
            let head = self.writing.load(Ordering::Acquire);
            let block = unsafe { &*head };

            debug_delay();
            let offset = block.len.fetch_add(1, Ordering::SeqCst);

            if offset < MAX_QUEUE_ITEMS {
                debug_delay();
                unsafe {
                    let ptr = (block.block.get() as *mut T).add(offset);
                    std::ptr::write(ptr, item);
                }

                // NB this bit set must happen after writing the item,
                // and it must have the `Release` ordering to ensure
                // it occurs after the pointer write.
                let necessary_bit: u64 = 1 << offset;
                block.complete.fetch_or(necessary_bit, Ordering::Release);

                return filled;
            } else {
                // install new writer
                let new = Box::into_raw(Box::new(OpBlock::default()));
                debug_delay();
                let prev =
                    self.writing.compare_and_swap(head, new, Ordering::SeqCst);
                if prev != head {
                    // we lost the CAS, free the new item that was
                    // never published to other threads
                    unsafe {
                        drop(Box::from_raw(new));
                    }
                    continue;
                }

                // push the now-full item to the full list for future consumption
                let mut ret;
                let mut full_list_ptr = self.full_list.load(Ordering::Acquire);
                while {
                    // we loop because maybe other threads are pushing stuff too
                    block.next.store(full_list_ptr, Ordering::SeqCst);
                    debug_delay();
                    ret = self.full_list.compare_and_swap(
                        full_list_ptr,
                        head,
                        Ordering::SeqCst,
                    );
                    ret != full_list_ptr
                } {
                    full_list_ptr = ret;
                }
                filled = true;
            }
        }
    }

    pub(crate) fn take<'a>(&self, guard: &'a Guard) -> OpIter<'a, T> {
        debug_delay();

        let mut ptr =
            self.full_list.swap(std::ptr::null_mut(), Ordering::SeqCst);
        let mut last = std::ptr::null_mut();

        // reverse the list for fifo iteration
        while !ptr.is_null() {
            let current_block = unsafe { &*ptr };
            let next = current_block.next.swap(last, Ordering::SeqCst);
            last = ptr;
            ptr = next;
        }

        OpIter { guard, current_offset: 0, current_block: last }
    }
}

impl<T: Copy> Drop for OpQueue<T> {
    fn drop(&mut self) {
        debug_delay();
        let writing = self.writing.load(Ordering::Acquire);
        unsafe {
            Box::from_raw(writing);
        }
        debug_delay();
        let mut head = self.full_list.load(Ordering::Acquire);
        while !head.is_null() {
            unsafe {
                debug_delay();
                let next =
                    (*head).next.swap(std::ptr::null_mut(), Ordering::SeqCst);
                Box::from_raw(head);
                head = next;
            }
        }
    }
}

pub(crate) struct OpIter<'a, T: Copy> {
    guard: &'a Guard,
    current_offset: usize,
    current_block: *mut OpBlock<T>,
}

impl<'a, T: Copy + 'static> Iterator for OpIter<'a, T> {
    type Item = T;

    fn next(&mut self) -> Option<T> {
        while !self.current_block.is_null() {
            let current_block = unsafe { &*self.current_block };

            debug_delay();
            if self.current_offset >= MAX_QUEUE_ITEMS {
                let to_drop = unsafe { Box::from_raw(self.current_block) };
                debug_delay();
                self.current_block = current_block.next.load(Ordering::Acquire);
                self.current_offset = 0;
                debug_delay();
                self.guard.defer(|| to_drop);
                continue;
            }

            let necessary_bit: u64 = 1 << self.current_offset;

            while current_block.complete.load(Ordering::Acquire) & necessary_bit
                == 0
            {
                // the submitter is not done with inserting the item yet
                spin_loop_hint();
            }

            let next: T = unsafe {
                let next_ptr: *const T = (current_block.block.get() as *mut T)
                    .add(self.current_offset);
                std::ptr::read(next_ptr)
            };
            self.current_offset += 1;
            return Some(next);
        }

        None
    }
}
